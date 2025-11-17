package distributedsorting.worker

import distributedsorting.distributedsorting._
import java.nio.file.Path
import scala.collection.mutable

// 파일 요청과정에서 backpressure 예방을 위해
// parallelism 조절을 위한 추상메서드
trait ShuffleStrategy {
    /**
     * needed_file 집합에 대해 shuffle을 수행
     *
     * @param neededFiles 가져와야 할 파일들
     * @param fileStructure 파일 구조 정보
     * @param shuffleOutputDir shuffle 결과를 저장할 디렉토리
     * @param fileTransport 파일 전송 인터페이스
     * @return 성공적으로 가져온 파일 수
     */
    def execute(
        neededFiles: mutable.Set[FileId],
        fileStructure: ???,
        shuffleOutputDir: Path,
        fileTransport: FileTransport
    ): Int
}

// =============================================================================
// 순차 전략 (Sequential)
// =============================================================================

/**
 * 순차 Shuffle 전략
 *
 * 한 번에 하나의 파일씩 순차적으로 요청
 *
 * 장점: 구현이 간단하고 안정적, 네트워크/메모리 부담이 적음
 * 단점: 느림 (많은 파일이 있을 경우)
 * 사용: 소규모 데이터, 안정성 우선
 */
class SequentialShuffleStrategy extends ShuffleStrategy {
    override def execute(
        neededFiles: mutable.Set[FileId],
        fileStructure: FileStructure,
        shuffleOutputDir: Path,
        fileTransport: FileTransport
    ): Int = {

        var successCount = 0

        // fileTransport.requestFile 이용하여 구현
        while (neededFiles.nonEmpty) {
            val fileId = neededFiles.head
            val destPath = shuffleOutputDir.resolve(fileId.toFileName)

            if (fileTransport.requestFile(fileId, destPath)) {
                neededFiles -= fileId
                successCount += 1
            } else {
                neededFiles -= fileId
            }
        }
        successCount
    }
}

// 로컬환경, 원격환경 모두에 대해 sequential 방식으로 테스트 통과 후 구현 예정
class LimitedConcurrencyShuffleStrategy extends ShuffleStrategy {
    override def execute(
        neededFiles: mutable.Set[FileId],
        fileStructure: FileStructure,
        shuffleOutputDir: Path,
        fileTransport: FileTransport
    ): Int = {
        // TODO: 제한된 동시성으로 파일 요청
        0
    }
}
