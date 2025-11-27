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

// =============================================================================
// Per-Worker 동시성 전략 (각 워커당 1개씩 동시 다운로드)
// =============================================================================

/**
 * Per-Worker 동시성 Shuffle 전략
 *
 * 각 소스 워커로부터 동시에 1개씩 파일을 다운로드
 * 예: 20개 워커 → 동시에 20개 파일 다운로드 (각 워커당 1개)
 *
 * @param filesPerWorker 각 워커로부터 동시에 다운로드할 파일 수 (기본값: 1)
 *
 * 장점:
 *   - 모든 워커의 업로드 대역폭을 골고루 활용
 *   - 특정 워커에 부하 집중 방지
 *   - 네트워크 균형적 사용
 *   - 워커 개수만큼 자동으로 병렬성 확장
 *
 * 단점:
 *   - 워커가 많으면 동시 전송 파일 수가 많아짐 (메모리 사용 증가)
 *
 * 사용: 대규모 클러스터 환경에서 권장
 */
class PerWorkerShuffleStrategy(filesPerWorker: Int = 1) extends ShuffleStrategy {
    override def execute(
        neededFiles: mutable.Set[FileId],
        shuffleOutputDir: Path,
        fileTransport: FileTransport
    ): Int = {
        import scala.concurrent.{Future, Await, ExecutionContext}
        import scala.concurrent.duration._
        import ExecutionContext.Implicits.global

        val fileList = neededFiles.toList

        // 1. 소스 워커별로 파일 그룹화
        val filesBySourceWorker = fileList.groupBy(_.sourceWorkerId)
        val numWorkers = filesBySourceWorker.keys.size

        println(s"[PerWorkerStrategy] Shuffling ${fileList.size} files from $numWorkers workers")
        println(s"[PerWorkerStrategy] Strategy: $filesPerWorker file(s) per worker concurrently")
        println(s"[PerWorkerStrategy] Max concurrent downloads: ${numWorkers * filesPerWorker}")

        var totalSuccess = 0
        var roundNumber = 0

        // 2. 각 워커별로 파일 큐 생성
        val workerQueues = filesBySourceWorker.map { case (workerId, files) =>
            workerId -> mutable.Queue(files: _*)
        }

        // 3. 모든 큐가 비워질 때까지 반복
        while (workerQueues.values.exists(_.nonEmpty)) {
            roundNumber += 1

            // 4. 각 워커로부터 filesPerWorker개씩 가져올 파일 선택
            val currentBatch = mutable.ArrayBuffer[FileId]()

            workerQueues.foreach { case (workerId, queue) =>
                // 각 워커의 큐에서 최대 filesPerWorker개 가져오기
                val filesToFetch = (1 to filesPerWorker).flatMap { _ =>
                    if (queue.nonEmpty) Some(queue.dequeue()) else None
                }
                currentBatch ++= filesToFetch
            }

            if (currentBatch.isEmpty) {
                // 모든 큐가 비었으면 종료
                println(s"[PerWorkerStrategy] All queues empty, finishing")
            } else {
                println(s"[PerWorkerStrategy] Round $roundNumber: Fetching ${currentBatch.size} files concurrently")

                // 5. 선택된 파일들을 병렬로 다운로드
                val futures = currentBatch.map { fileId =>
                    Future {
                        val destPath = shuffleOutputDir.resolve(fileId.toFileName)
                        val startTime = System.currentTimeMillis()
                        val result = fileTransport.requestFile(fileId, destPath)
                        val elapsed = System.currentTimeMillis() - startTime

                        if (result) {
                            println(s"[PerWorkerStrategy] ✓ Worker ${fileId.sourceWorkerId} → ${fileId.toFileName} (${elapsed}ms)")
                        } else {
                            println(s"[PerWorkerStrategy] ✗ Worker ${fileId.sourceWorkerId} → ${fileId.toFileName} FAILED")
                        }

                        (fileId, result)
                    }
                }

                // 6. 현재 라운드의 모든 다운로드 완료 대기
                try {
                    val results = Await.result(Future.sequence(futures), 120.seconds)
                    val roundSuccess = results.count(_._2 == true)
                    totalSuccess += roundSuccess

                    println(s"[PerWorkerStrategy] Round $roundNumber completed: $roundSuccess/${currentBatch.size} succeeded")

                    // 워커별 성공률 출력
                    val successByWorker = results.filter(_._2).groupBy(_._1.sourceWorkerId).mapValues(_.size)
                    val failByWorker = results.filterNot(_._2).groupBy(_._1.sourceWorkerId).mapValues(_.size)

                    successByWorker.toSeq.sortBy(_._1).foreach { case (wId, count) =>
                        val failed = failByWorker.getOrElse(wId, 0)
                        if (failed > 0) {
                            println(s"[PerWorkerStrategy]   Worker $wId: $count succeeded, $failed failed")
                        }
                    }

                } catch {
                    case e: Exception =>
                        println(s"[PerWorkerStrategy] Round $roundNumber failed: ${e.getMessage}")
                        e.printStackTrace()
                }
            }
        }

        println(s"[PerWorkerStrategy] Complete: $totalSuccess/${fileList.size} files succeeded in $roundNumber rounds")
        println(s"[PerWorkerStrategy] Files per worker per round: $filesPerWorker")

        totalSuccess
    }
}
