package distributedsorting.worker

import scopt.OptionParser
import java.net.InetAddress
import scala.concurrent.ExecutionContext
import io.grpc.{Server, ServerBuilder}
import com.typesafe.config.ConfigFactory
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import java.nio.file.{Path, Paths}
import distributedsorting.distributedsorting._
import distributedsorting.logic._
import distributedsorting.worker.TestHelpers.{FileStructure, ShuffleResult}

object Worker {
    val config = ConfigFactory.load()
    val configPath = "distributedsorting"

    def main(args: Array[String]): Unit = {
        WorkerArgsParser.parser.parse(ArgsUtils.normalizeInputDirectoriesArgs(args), WorkerConfig()) match {
            case Some(config) =>
                val Array(masterIp, masterPortStr) = config.masterAddress.split(":")
                val masterPort = masterPortStr.toInt
                
                println(s"Master IP: $masterIp, Port: $masterPort")
                println(s"Input Directories: ${config.inputDirs.mkString(", ")}")
                println(s"Output Directory: ${config.outputDir}")
                
                val workerApp = new WorkerApp(masterIp, masterPort, config.inputDirs, config.outputDir)
                workerApp.run()

            case None =>
                println("Argument parsing failed.")
        }
    }
}

class WorkerApp (
  ip: String,
  port: Int,
  inputDirsStr: Seq[String],
  outputDirStr: String
) extends MasterClient with ExternalSorter{   
  implicit val ec: ExecutionContext = ExecutionContext.global  
  val config = ConfigFactory.load()
  val configPath = "distributedsorting"
  val workerIp: String = InetAddress.getLocalHost.getHostAddress
  var workerPort = 0 // run 이후 설정 됨

  var workerId = -1

  val inputDirs: Seq[Path] = inputDirsStr.map(Paths.get(_))
  val outputDir: Path = Paths.get(outputDirStr)
  val tempDir: Path = outputDir.resolve("temp")

  val partitionOutputDir: Path = tempDir.resolve("partition-output")
  val shuffleOutputDir: Path = tempDir.resolve("shuffle-output")
  val mergeTempDir: Path = tempDir.resolve("external-sorter-temp")
  
  // worker server 생성  
  val workerService = new WorkerServiceImpl(partitionOutputDir)
  val server = ServerBuilder.forPort(0)
      .addService(WorkerServiceGrpc.bindService(workerService, ec))
      .build()
  
  override val masterIp = ip
  override val masterPort = port
  override lazy val workerRegisterInfo = new WorkerInfo(workerId = -1, ip = workerIp, port = workerPort)
  
  override val KEY_SIZE = config.getInt(s"$configPath.record-info.key-length")
  override val RECORD_SIZE = config.getInt(s"$configPath.record-info.record-length")

  var pivots: Vector[Record] = _

  // for shuffle
  val shuffleStrategy = new SequentialShuffleStrategy()

  // for ExternalSorter
  val externalSorterInputDirectory: Path = shuffleOutputDir
  val externalSorterOutputDirectory: Path = outputDir
  val externalSorterTempDirectory: Path = mergeTempDir
  val externalSorterOrdering: Ordering[Record] = createRecordOrdering(KEY_SIZE, KEY_SIZE)
  val chunkSize: Long = config.getBytes(s"$configPath.external-sort.chunk-size").toLong  
  val outputPrefix: String = config.getString(s"$configPath.external-sort.output-prefix")
  val outputStartPostfix: Int = config.getInt(s"$configPath.external-sort.output-start-postfix")
  val MEMORY_SIZE: Long = config.getBytes(s"$configPath.cluster-info.node-info.memory").toLong
  val EXTERNAL_SORT_USABLE_MEMORY_RATIO: Double = config.getDouble(s"$configPath.external-sort.max-memory-usage-ratio")
  val BUFFER_SIZE: Long = config.getBytes(s"$configPath.io.buffer-size").toLong


  def run(): Unit = {
    server.start()
    workerPort = server.getPort

    // worker registration
    registerWorker()
    workerId = workerInfo.workerId
    workerService.registerWorkerId(workerId)
    println(s"complete registration")
    println(s"my info: id $workerId, ip ${workerInfo.ip}, port ${workerInfo.port}")
    println("========== all workers ==========")
    getAllWorkers.foreach(w => println(s"id ${w.workerId}, ip ${w.ip}, port ${w.port}"))
    println("=================================")

    // Sampling
    pivots = executeSampling(inputDirs)

    // TODO: Sort&Partition
    val localFileIds: Set[FileId] = FileStructureManager.collectLocalFileIds(partitionOutputDir)
    val fileStructure: FileStructure = reportFileIds(localFileIds)

    // Shuffle
    val partitionFileStructure: FileStructure = fileStructure
    var workerAddresses: Map[Int, String] = getAllWorkers.map { w =>
      w.workerId -> s"${w.ip}:${w.port}"
    }.toMap
    var remoteFileTransport = new RemoteFileTransport(workerId, partitionOutputDir, workerAddresses)
    remoteFileTransport.init() // 다른 워커 서버 연결

    val result: ShuffleResult = shufflePhase(
                        partitionId = workerId,
                        fileStructure = partitionFileStructure,
                        fileTransport = remoteFileTransport,
                        getFiles = (fs: FileStructure) => fs.getFilesForPartition(workerId),
                        buildResult = (success: Int, failure: Int) => ShuffleResult(success, failure)
                    )
    
    try {
      remoteFileTransport.close()
    } catch {
      case e: Throwable => println("Error: cannot close connection")
    }

    // Merge
    executeExternalSort()

    reportCompletion()

    server.shutdown()

    // TODO: temp 파일, 폴더 정리
  }

  /**
     * Shuffle Phase 실행
     * 
     * 제네릭 타입을 사용하여 FileStructure와 ShuffleResult 타입에 독립적
     * 
     * @param partitionId 이 워커가 담당할 파티션 ID
     * @param fileStructure 파일 구조 정보 (타입 FS는 테스트/실제 구현에서 결정)
     * @param fileTransport fileTransport 객체
     * @param getFiles FileStructure에서 필요한 파일들을 추출하는 함수
     * @param buildResult 성공/실패 카운트로 결과 객체를 생성하는 함수
     * @return 결과 객체 (타입 R은 테스트/실제 구현에서 결정)
     */
    def shufflePhase[FS, R](
        partitionId: Int, 
        fileStructure: FS,
        fileTransport: FileTransport,
        getFiles: FS => Set[FileId],          // FileStructure → 필요한 파일들
        buildResult: (Int, Int) => R          // (성공 수, 실패 수) → 결과
    ): R = {
        // 1. needed_file 초기화
        val neededFiles = getFiles(fileStructure).to(mutable.Set)
        
        // 2. ShuffleStrategy를 사용하여 파일 요청
        // val shuffleOutputDir 클래스 변수에 정의됨
        
        val successCount = shuffleStrategy.execute(
            neededFiles,
            shuffleOutputDir,
            fileTransport
        )
        
        val failureCount = getFiles(fileStructure).size - successCount
        
        println(s"Worker $workerId: Successfully fetched $successCount files")
        
        buildResult(successCount, failureCount)
    }
}

case class WorkerConfig(
    masterAddress: String = "",
    inputDirs: Seq[String] = Seq.empty,
    outputDir: String = ""
)

object WorkerArgsParser {
    val parser = new OptionParser[WorkerConfig]("worker") {
    
    head("worker", "Distributed Sort Worker Node")

    // 1. Master IP:Port (위치 인자) 처리
    // 첫 번째 위치 인자를 masterAddress로 설정
    arg[String]("<master IP:port>")
      .required()
      .action { (x, c) => c.copy(masterAddress = x) }
      .text("Master IP and port (e.g., 141.223.91.80:30040)")

    // 2. 입력 디렉토리 옵션 (-I) 처리
    // Repetition: 여러 개의 디렉토리를 목록으로 받음
    opt[Seq[String]]('I', "input-directories")
      .required()
      .valueName("<dir1> <dir2>...")
      .unbounded() // 인자가 무한정 올 수 있음을 명시
      .action { (x, c) => 
        c.copy(inputDirs = c.inputDirs ++ x.filter(_.nonEmpty)) 
      }
      .text("Input directories containing unsorted input blocks.")

    // 3. 출력 디렉토리 옵션 (-O) 처리
    opt[String]('O', "output-directory")
      .required()
      .valueName("<dir>")
      .action { (x, c) => c.copy(outputDir = x) }
      .text("Output directory to store sorted partition files.")
      
    help("help").text("prints this usage text")
  }
}

object ArgsUtils {
  def normalizeInputDirectoriesArgs(args: Array[String]): Array[String] = {
    val newArgs = ArrayBuffer[String]()
    var i = 0
    
    while (i < args.length) {
      val arg = args(i)
      // -I 또는 --input-directories 플래그를 만났을 때
      if (arg == "-I" || arg == "--input-directories") {
        newArgs += arg
        i += 1
        
        // 다음 인자들이 옵션(-)이 아닐 동안 계속 수집해서 콤마로 합침
        val dirs = ArrayBuffer[String]()
        while (i < args.length && !args(i).startsWith("-")) {
          dirs += args(i)
          i += 1
        }
        // 수집한 경로들을 콤마로 묶어서 하나의 인자로 만듦
        if (dirs.nonEmpty) {
          newArgs += dirs.mkString(",") 
        }
      } else {
        // 그 외의 인자(Master IP, -O 등)는 그대로 통과
        newArgs += arg
        i += 1
      }
    }
    newArgs.toArray
  }
}