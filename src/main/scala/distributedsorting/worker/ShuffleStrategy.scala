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
     * @param shuffleOutputDir shuffle 결과를 저장할 디렉토리
     * @param fileTransport 파일 전송 인터페이스
     * @return 성공적으로 가져온 파일 수
     */
    def execute(
        neededFiles: mutable.Set[FileId],
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

/**
 * 제한된 동시성 Shuffle 전략
 *
 * 여러 파일을 동시에 요청하되, 최대 동시성을 제한하여 네트워크/메모리 과부하 방지
 *
 * @param maxConcurrency 동시에 전송할 최대 파일 수 (기본값: 10)
 *
 * 장점: 순차 방식보다 훨씬 빠름, 네트워크 대역폭 효율적 사용
 * 단점: 메모리 사용량 증가 (동시 전송 파일 수만큼)
 * 사용: 일반적인 경우 권장
 */
class LimitedConcurrencyShuffleStrategy(maxConcurrency: Int = 10) extends ShuffleStrategy {
    override def execute(
        neededFiles: mutable.Set[FileId],
        shuffleOutputDir: Path,
        fileTransport: FileTransport
    ): Int = {
        import scala.concurrent.{Future, Await, ExecutionContext}
        import scala.concurrent.duration._
        import ExecutionContext.Implicits.global

        var successCount = 0
        val fileList = neededFiles.toList

        println(s"[LimitedConcurrencyShuffleStrategy] Shuffling ${fileList.size} files with concurrency=$maxConcurrency")

        // maxConcurrency개씩 배치로 나눠서 처리
        fileList.grouped(maxConcurrency).zipWithIndex.foreach { case (batch, batchIndex) =>
            println(s"[LimitedConcurrencyShuffleStrategy] Processing batch ${batchIndex + 1} (${batch.size} files)")

            // 배치 내의 파일들을 병렬로 요청
            val futures = batch.map { fileId =>
                Future {
                    val destPath = shuffleOutputDir.resolve(fileId.toFileName)
                    val result = fileTransport.requestFile(fileId, destPath)
                    (fileId, result)
                }
            }

            // 배치가 완료될 때까지 대기 (최대 120초)
            try {
                val results = Await.result(Future.sequence(futures), 120.seconds)
                val batchSuccessCount = results.count(_._2 == true)
                successCount += batchSuccessCount
                println(s"[LimitedConcurrencyShuffleStrategy] Batch ${batchIndex + 1} completed: $batchSuccessCount/${batch.size} succeeded")
            } catch {
                case e: Exception =>
                    println(s"[LimitedConcurrencyShuffleStrategy] Batch ${batchIndex + 1} failed: ${e.getMessage}")
                    e.printStackTrace()
            }
        }

        println(s"[LimitedConcurrencyShuffleStrategy] Total: $successCount/${fileList.size} files succeeded")
        successCount
    }
}
