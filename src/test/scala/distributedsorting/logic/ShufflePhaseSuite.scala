package distributedsorting.logic

import distributedsorting.distributedsorting._
import distributedsorting.worker._
import distributedsorting.worker.TestHelpers._
import distributedsorting.logic._
import munit.FunSuite
import java.nio.file.{Path, Files}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

class ShufflePhaseTest extends FunSuite {
    implicit val ec: ExecutionContext = ExecutionContext.global
    
    val NUM_WORKERS = 4
    val RECORD_SIZE = 100
    val KEY_SIZE = 10
    val RECORDS_PER_FILE = 1000

    /**
     * 테스트용 파티션 파일 생성
     * 각 워커가 이미 파티션을 완료했다고 가정
     */
    def setupPartitionFiles(testDir: Path): Unit = {
        (0 until NUM_WORKERS).foreach { workerId =>
            val workerDir = testDir.resolve(s"worker_$workerId")
            val partitionDir = workerDir.resolve("partitions")
            Files.createDirectories(partitionDir)
            
            // 각 워커가 numWorkers개의 파티션 파일 생성
            (0 until NUM_WORKERS).foreach { partitionId =>
                val fileId = FileId(workerId, partitionId, 0)
                val filePath = partitionDir.resolve(fileId.toFileName)
                
                // 더미 레코드 생성 (파티션 ID를 키로 사용)
                val records = (0 until RECORDS_PER_FILE).map { i =>
                    val key = Array.fill[Byte](KEY_SIZE)((partitionId * 10 + i % 10).toByte)
                    val value = Array.fill[Byte](RECORD_SIZE - KEY_SIZE)(0.toByte)
                    key ++ value
                }
                
                // 파일 쓰기
                RecordWriterRunner.WriteRecordIterator(filePath, records.iterator)
                
                println(s"Created: ${fileId.toFileName} (${Files.size(filePath)} bytes)")
            }
        }
    }

    test("Shuffle phase: Workers exchange partition files") {
        val testDir = Files.createTempDirectory("shuffle-test")
        try {
            // ============================================
            // 1. 사전 준비: 파티션 파일 생성
            // ============================================
            setupPartitionFiles(testDir)

            // ============================================
            // 2. WorkerRegistry 생성 (공유 객체) - TestHelpers 사용
            // ============================================
            val registry = WorkerRegistry()  // TestHelpers.WorkerRegistry

            // ============================================
            // 3. Worker 생성 및 시작
            // ============================================
            val workers = (0 until NUM_WORKERS).map { workerId =>
                val workerDir = testDir.resolve(s"worker_$workerId")
                Files.createDirectories(workerDir.resolve("shuffle_output"))
                
                // LocalFileTransport 생성
                val transport = new LocalFileTransport(
                    workerId = workerId,
                    sharedDirectory = testDir,
                    registry = registry
                )
                
                // ShuffleStrategy 생성
                val strategy = new SequentialShuffleStrategy()
                
                // Worker 생성
                val worker = new Worker(
                    workerId = workerId,
                    numWorkers = NUM_WORKERS,
                    workingDir = workerDir,
                    inputDirs = Seq.empty,  // 셔플 테스트에서는 불필요
                    fileTransport = transport,
                    shuffleStrategy = strategy
                )
                
                // Worker 시작 (Service Thread 시작)
                worker.start()

                worker
            }
            
            println(s"\n[Test] All ${NUM_WORKERS} workers started")

            // ============================================
            // 4. FileStructure 생성 (Map O/D) - TestHelpers 사용
            // ============================================
            val fileStructure = createTestFileStructure(NUM_WORKERS)
            
            println(s"[Test] FileStructure created: ${fileStructure.allFiles.size} files")

            // ============================================
            // 5. 각 워커가 shufflePhase 실행 (병렬)
            // ============================================
            // Future 병렬실행으로 내부적으로 각 워커객체는 서로 다른 thread에서 실행됨
            val shuffleFutures = workers.zipWithIndex.map { case (worker, myPartitionId) =>
                Future {
                    println(s"[Worker ${worker.workerId}] Shuffling partition $myPartitionId")
                    
                    // 제네릭 shufflePhase 호출
                    val result: ShuffleResult = worker.shufflePhase(
                        partitionId = myPartitionId,
                        fileStructure = fileStructure,
                        getFiles = (fs: FileStructure) => fs.getFilesForPartition(myPartitionId),
                        buildResult = (success: Int, failure: Int) => ShuffleResult(success, failure)
                    )
                    
                    println(s"[Worker ${worker.workerId}] Shuffle complete: " +
                            s"${result.successCount} success, ${result.failureCount} failed")
                    
                    (worker.workerId, result)
                }
            }
            
            // 모든 셔플 완료 대기
            val results = Await.result(Future.sequence(shuffleFutures), 2.minutes)

            // ============================================
            // 6. 검증
            // ============================================
            println(s"\n[Test] Verifying results...")
            
            results.foreach { case (workerId, result) =>
                // 성공 개수 확인
                assertEquals(
                    result.successCount, 
                    NUM_WORKERS,
                    s"Worker $workerId should download $NUM_WORKERS files"
                )
                
                // 실패 없음
                assertEquals(
                    result.failureCount,
                    0,
                    s"Worker $workerId should have no failures"
                )
                
                // 실제 파일 확인
                val shuffleDir = testDir.resolve(s"worker_$workerId").resolve("shuffle_output")
                val downloadedFiles = Files.list(shuffleDir).count()
                
                assertEquals(
                    downloadedFiles,
                    NUM_WORKERS.toLong,
                    s"Worker $workerId should have $NUM_WORKERS files in shuffle_output"
                )
                
                // 파일 크기 확인
                (0 until NUM_WORKERS).foreach { sourceWorkerId =>
                    val fileName = s"file_${sourceWorkerId}_${workerId}_0.dat"
                    val filePath = shuffleDir.resolve(fileName)
                    
                    assert(Files.exists(filePath), s"File $fileName should exist")
                    
                    val expectedSize = RECORDS_PER_FILE * RECORD_SIZE
                    assertEquals(
                        Files.size(filePath),
                        expectedSize.toLong,
                        s"File $fileName size mismatch"
                    )
                }
            }
            
            println(s"\n[Test] ✓ All verifications passed!")
            
            // ============================================
            // 7. 정리
            // ============================================
            workers.foreach(_.shutdown())
            registry.shutdown()
            
        } finally {
            deleteRecursively(testDir)
        }
    }

    /**
     * 디렉토리 재귀 삭제
     */
    def deleteRecursively(path: Path): Unit = {
        if (Files.isDirectory(path)) {
            val stream = Files.list(path)
            try {
                stream.forEach(deleteRecursively)
            } finally {
                stream.close()
            }
        }
        Files.deleteIfExists(path)
    }
}
