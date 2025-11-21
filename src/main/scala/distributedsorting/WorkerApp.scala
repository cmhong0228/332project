package distributedsorting

import distributedsorting._
import worker._
import worker.TestHelpers._
import java.nio.file.{Files, Path, Paths}
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * 개별 워커를 실행하는 애플리케이션
 * 사용법: sbt "runMain distributedsorting.WorkerApp 0 3"
 * 
 * 변경사항:
 * - 포트는 자동 할당됨 (더 이상 인자로 받지 않음)
 * - Worker API 변경에 맞춰 수정
 */
object WorkerApp extends App {
  
  if (args.length < 2) {
    println("Usage: WorkerApp <workerId> <numWorkers>")
    println("Example: WorkerApp 0 3")
    System.exit(1)
  }
  
  val workerId = args(0).toInt
  val numWorkers = args(1).toInt
  
  val RECORDS_PER_FILE = 1000
  val RECORD_SIZE = 100
  val FILES_PER_PARTITION = 3  // 각 파티션당 생성할 파일 개수
  
  println(s"=" * 60)
  println(s"Starting Worker $workerId")
  println(s"  Total workers: $numWorkers")
  println(s"=" * 60)
  
  // 작업 디렉토리 설정
  val baseDir = Paths.get(System.getProperty("user.dir"), "test-data", "multi-process")
  val workerDir = baseDir.resolve(s"worker_$workerId")
  val partitionDir = workerDir.resolve("partitions")
  val shuffleDir = workerDir.resolve("shuffle_output")
  
  // 디렉토리 생성
  Files.createDirectories(partitionDir)
  Files.createDirectories(shuffleDir)
  
  println(s"[Worker $workerId] Working directory: $workerDir")
  
  // ============================================
  // 1. Map Phase 시뮬레이션: 자신의 파티션 파일 생성
  // ============================================
  println(s"[Worker $workerId] Step 1: Creating my partition files (simulating Map phase)...")
  createMyPartitionFiles(partitionDir, workerId, numWorkers)
  
  // ============================================
  // 2. Worker 생성 및 gRPC 서버 시작
  // ============================================
  val worker = new Worker(
    workerId = workerId,
    numWorkers = numWorkers,
    workingDir = workerDir,
    inputDirs = Seq.empty
  )
  
  // gRPC 서버 시작 및 포트 획득
  val port = worker.start(partitionDir)
  println(s"[Worker $workerId] ✓ gRPC server started on port $port")
  
  // 포트 정보를 파일로 저장 (Coordinator가 읽을 수 있도록)
  val portFile = baseDir.resolve(s"port_worker_$workerId.txt")
  Files.write(portFile, port.toString.getBytes)
  println(s"[Worker $workerId] ✓ Port info saved: $portFile")
  
  // 준비 완료 신호
  val readyFile = baseDir.resolve(s"ready_worker_$workerId")
  Files.createFile(readyFile)
  println(s"[Worker $workerId] ✓ Ready signal sent")
  
  // ============================================
  // 3. 워커 주소 정보 대기
  // ============================================
  println(s"[Worker $workerId] Waiting for worker addresses from coordinator...")
  val addressFile = baseDir.resolve(s"addresses_worker_$workerId.txt")
  while (!Files.exists(addressFile)) {
    Thread.sleep(500)
  }
  
  // 주소 파일 읽기 (형식: workerId=host:port, 한 줄씩)
  val workerAddresses = scala.io.Source.fromFile(addressFile.toFile)
    .getLines()
    .map { line =>
      val Array(id, address) = line.split("=")
      id.toInt -> address
    }
    .toMap
  
  println(s"[Worker $workerId] ✓ Received worker addresses: $workerAddresses")
  
  // 워커 주소 설정
  worker.setWorkerAddresses(workerAddresses)
  
  // ============================================
  // 4. Shuffle Phase 준비
  // ============================================
  println(s"[Worker $workerId] Preparing shuffle phase...")
  worker.prepareShufflePhase(
    useRemote = true,
    partitionDir = partitionDir,
    registry = None
  )
  println(s"[Worker $workerId] ✓ Shuffle phase prepared")
  
  // ============================================
  // 5. 시작 신호 대기
  // ============================================
  println(s"[Worker $workerId] Waiting for coordinator start signal...")
  val startSignalFile = baseDir.resolve(s"start_worker_$workerId")
  while (!Files.exists(startSignalFile)) {
    Thread.sleep(500)
  }
  
  println(s"[Worker $workerId] ✓ Start signal received!")
  
  // ============================================
  // 6. Shuffle Phase 실행
  // ============================================
  val fileStructure = createTestFileStructureMultiIndex(numWorkers, FILES_PER_PARTITION)
  
  println(s"[Worker $workerId] Starting shuffle phase...")
  println(s"[Worker $workerId] Expected files: ${numWorkers * FILES_PER_PARTITION}")
  
  val result: ShuffleResult = worker.shufflePhase(
    partitionId = workerId,
    fileStructure = fileStructure,
    getFiles = (fs: FileStructure) => fs.getFilesForPartition(workerId),
    buildResult = (success: Int, failure: Int) => ShuffleResult(success, failure)
  )
  
  println(s"[Worker $workerId] ✓ Shuffle complete!")
  println(s"[Worker $workerId]   Success: ${result.successCount}")
  println(s"[Worker $workerId]   Failure: ${result.failureCount}")
  
  // 완료 신호
  val doneFile = baseDir.resolve(s"done_worker_$workerId")
  Files.createFile(doneFile)
  
  println(s"[Worker $workerId] Keeping server running... (Press Ctrl+C to exit)")
  
  // 종료 신호 대기
  Runtime.getRuntime.addShutdownHook(new Thread {
    override def run(): Unit = {
      println(s"\n[Worker $workerId] Shutting down...")
      worker.shutdown()
      println(s"[Worker $workerId] ✓ Shutdown complete")
    }
  })
  
  Thread.sleep(Long.MaxValue)
  
  /**
   * 이 Worker가 생성해야 할 파티션 파일들 생성
   * (Map Phase 시뮬레이션)
   * 각 타겟 파티션에 대해 여러 개의 index 파일 생성
   */
  def createMyPartitionFiles(partitionDir: Path, myWorkerId: Int, numWorkers: Int): Unit = {
    println(s"[Worker $myWorkerId] Creating ${FILES_PER_PARTITION} files per partition...")
    
    // 각 타겟 파티션에 대해
    (0 until numWorkers).foreach { targetPartition =>
      // 여러 개의 index 파일 생성
      (0 until FILES_PER_PARTITION).foreach { fileIndex =>
        val fileId = FileId(myWorkerId, targetPartition, fileIndex)
        val filePath = partitionDir.resolve(fileId.toFileName)
        
        // 더미 데이터 생성 (실제로는 Map 결과)
        // 각 파일마다 다른 데이터 (fileIndex를 seed로 사용)
        val data = new Array[Byte](RECORDS_PER_FILE * RECORD_SIZE)
        val random = new scala.util.Random(myWorkerId * 1000 + targetPartition * 10 + fileIndex)
        random.nextBytes(data)
        Files.write(filePath, data)
        
        println(s"[Worker $myWorkerId] Created: ${fileId.toFileName} (${data.length} bytes)")
      }
    }
    
    val totalFiles = numWorkers * FILES_PER_PARTITION
    println(s"[Worker $myWorkerId] ✓ Created $totalFiles partition files total")
  }
}
