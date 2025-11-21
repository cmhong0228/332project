package distributedsorting.worker

import distributedsorting.distributedsorting._
import java.nio.file.Path
import scala.collection.mutable
import scala.concurrent.ExecutionContext

/**
 * 분산 정렬 시스템의 워커 노드
 * 
 * 주의: shufflePhase의 타입들(FileStructure, ShuffleResult)은 
 * 현재 테스트용 임시 타입입니다. (TestHelpers 참조)
 * 실제 구현 시 다른 팀과 합의하여 정식 타입으로 교체될 예정입니다.
 */
class Worker(
    val workerId: Int,
    val numWorkers: Int,
    val workingDir: Path,               // 워커의 작업 디렉토리
    val inputDirs: Seq[Path]            // 입력 데이터 디렉토리들
)(implicit ec: ExecutionContext) {

    // gRPC 서버 (다른 워커로부터 파일 요청을 받음)
    private var workerService: Option[WorkerServiceImpl] = None
    
    // 워커 주소 정보 (등록 후 Master로부터 받음)
    private var workerAddresses: Map[Int, String] = Map.empty
    
    // 파일 전송 인터페이스 (셔플 단계에서 생성됨)
    private var fileTransport: Option[FileTransport] = None
    
    // 셔플 전략
    private var shuffleStrategy: ShuffleStrategy = new SequentialShuffleStrategy()

    /**
     * 워커 시작 (gRPC 서버 시작)
     * 
     * @param partitionDir 이 워커의 파티션 디렉토리
     * @return 할당된 포트 번호 (Master에게 등록할 때 사용)
     */
    def start(partitionDir: Path): Int = {
        // gRPC 서버 시작
        val service = new WorkerServiceImpl(workerId, partitionDir)
        val port = service.start()
        workerService = Some(service)
        
        println(s"[Worker $workerId] Started gRPC server on port $port")
        port
    }

    /**
     * Master로부터 받은 워커 주소 정보 설정
     * 
     * @param addresses workerId -> "host:port" 매핑
     */
    def setWorkerAddresses(addresses: Map[Int, String]): Unit = {
        workerAddresses = addresses
        println(s"[Worker $workerId] Worker addresses set: $addresses")
    }

    /**
     * Shuffle Phase 준비 (FileTransport 생성 및 초기화)
     * 
     * 이 메서드는 shufflePhase 실행 전에 호출되어야 함
     * 
     * @param useRemote true면 RemoteFileTransport, false면 LocalFileTransport
     * @param partitionDir 이 워커의 파티션 디렉토리
     * @param registry LocalFileTransport 사용 시 필요한 레지스트리
     */
    def prepareShufflePhase(
        useRemote: Boolean,
        partitionDir: Path,
        registry: Option[LocalTransportRegistry] = None
    ): Unit = {
        val transport = if (useRemote) {
            // Remote: gRPC 클라이언트 사용
            require(workerAddresses.nonEmpty, "Worker addresses must be set before preparing remote shuffle")
            new RemoteFileTransport(workerId, partitionDir, workerAddresses)
        } else {
            // Local: 공유 레지스트리 사용
            require(registry.isDefined, "Registry must be provided for local shuffle")
            val testDir = workingDir.getParent // sharedDirectory는 worker_X의 부모
            new LocalFileTransport(workerId, testDir, registry.get)
        }
        
        transport.init()
        fileTransport = Some(transport)
        
        println(s"[Worker $workerId] Shuffle phase prepared (remote: $useRemote)")
    }

    /**
     * Shuffle Strategy 설정 (선택적)
     */
    def setShuffleStrategy(strategy: ShuffleStrategy): Unit = {
        shuffleStrategy = strategy
    }

    /**
     * 워커 종료 (리소스 정리)
     */
    def shutdown(): Unit = {
        fileTransport.foreach(_.close())
        workerService.foreach(_.shutdown())
        println(s"[Worker $workerId] Shutdown complete")
    }

    /**
     * Shuffle Phase 실행
     * 
     * 제네릭 타입을 사용하여 FileStructure와 ShuffleResult 타입에 독립적
     * 
     * @param partitionId 이 워커가 담당할 파티션 ID
     * @param fileStructure 파일 구조 정보 (타입 FS는 테스트/실제 구현에서 결정)
     * @param getFiles FileStructure에서 필요한 파일들을 추출하는 함수
     * @param buildResult 성공/실패 카운트로 결과 객체를 생성하는 함수
     * @return 결과 객체 (타입 R은 테스트/실제 구현에서 결정)
     */
    def shufflePhase[FS, R](
        partitionId: Int, 
        fileStructure: FS,
        getFiles: FS => Set[FileId],          // FileStructure → 필요한 파일들
        buildResult: (Int, Int) => R          // (성공 수, 실패 수) → 결과
    ): R = {
        require(fileTransport.isDefined, "FileTransport must be initialized before shuffle phase")
        
        // 1. needed_file 초기화
        val neededFiles = getFiles(fileStructure).to(mutable.Set)
        
        // 2. ShuffleStrategy를 사용하여 파일 요청
        val shuffleOutputDir = workingDir.resolve("shuffle_output")
        
        val successCount = shuffleStrategy.execute(
            neededFiles,
            shuffleOutputDir,
            fileTransport.get
        )
        
        val failureCount = getFiles(fileStructure).size - successCount
        
        println(s"[Worker $workerId] Successfully fetched $successCount files")
        
        buildResult(successCount, failureCount)
    }
}

object Worker {
    /**
     * 로컬 테스트용 워커 생성 팩토리 메서드
     */
    def createLocal(
        workerId: Int,
        numWorkers: Int,
        workingDir: Path,
        inputDirs: Seq[Path]
    )(implicit ec: ExecutionContext): Worker = {
        new Worker(workerId, numWorkers, workingDir, inputDirs)
    }
    
    /**
     * 원격 환경용 워커 생성 팩토리 메서드
     */
    def createRemote(
        workerId: Int,
        numWorkers: Int,
        workingDir: Path,
        inputDirs: Seq[Path]
    )(implicit ec: ExecutionContext): Worker = {
        new Worker(workerId, numWorkers, workingDir, inputDirs)
    }
}
