package distributedsorting.worker

import distributedsorting.distributedsorting._
import java.nio.file.Path
import scala.collection.mutable

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
    val inputDirs: Seq[Path],           // 입력 데이터 디렉토리들
    val workerAddresses: Map[Int, String] = Map.empty,  // workerId -> address
    fileTransport: FileTransport,       // 파일 전송 추상화 (DI)
    shuffleStrategy: ShuffleStrategy    // Shuffle 요청 전략 (DI)
) {

    /**
     * 워커 시작 (서비스 스레드 등 초기화)
     */
    def start(): Unit = {
        fileTransport.init()
        // TODO: 기타 초기화 작업
    }

    /**
     * 워커 종료 (리소스 정리)
     */
    def shutdown(): Unit = {
        // TODO: 리소스 정리 작업
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
        // 1. needed_file 초기화
        val neededFiles = getFiles(fileStructure).to(mutable.Set)
        
        // 2. ShuffleStrategy를 사용하여 파일 요청
        val shuffleOutputDir = workingDir.resolve("shuffle_output")
        
        val successCount = shuffleStrategy.execute(
            neededFiles,
            fileStructure,
            shuffleOutputDir,
            fileTransport
        )
        
        val failureCount = getFiles(fileStructure).size - successCount
        
        println(s"Worker $workerId: Successfully fetched $successCount files")
        
        buildResult(successCount, failureCount)
    }
}

// TODO: object Worker 선언 로직 (local버전 / remote 버전)
object Worker {
    // Factory methods can be added here
}
