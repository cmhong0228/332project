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

    test("Shuffle phase: Workers exchange partition files (Local)") {
        val testDir = Files.createTempDirectory("shuffle-test")
        try {
            // ============================================
            // 1. 사전 준비: 파티션 파일 생성
            // ============================================
            setupPartitionFiles(testDir)

            // ============================================
            // 2. WorkerRegistry 생성 (공유 객체)
            // ============================================
            val registry = WorkerRegistry()

            // ============================================
            // 3. Worker 생성
            // ============================================
            val workers = (0 until NUM_WORKERS).map { workerId =>
                val workerDir = testDir.resolve(s"worker_$workerId")
                Files.createDirectories(workerDir.resolve("shuffle_output"))
                
                val worker = Worker.createLocal(
                    workerId = workerId,
                    numWorkers = NUM_WORKERS,
                    workingDir = workerDir,
                    inputDirs = Seq.empty
                )
                
                worker
            }

            // ============================================
            // 4. Worker 시작 (gRPC 서버 시작) - Local에서는 생략 가능
            // ============================================
            workers.foreach { worker =>
                val partitionDir = testDir.resolve(s"worker_${worker.workerId}").resolve("partitions")
                // Local 테스트에서는 포트가 필요 없지만, 일관성을 위해 호출
                // worker.start(partitionDir)
            }
            
            println(s"\n[Test] All ${NUM_WORKERS} workers created")

            // ============================================
            // 5. Shuffle Phase 준비 (Local)
            // ============================================
            workers.foreach { worker =>
                val partitionDir = testDir.resolve(s"worker_${worker.workerId}").resolve("partitions")
                worker.prepareShufflePhase(
                    useRemote = false,
                    partitionDir = partitionDir,
                    registry = Some(registry)
                )
            }

            // ============================================
            // 6. FileStructure 생성
            // ============================================
            val fileStructure = createTestFileStructure(NUM_WORKERS)
            
            println(s"[Test] FileStructure created: ${fileStructure.allFiles.size} files")

            // ============================================
            // 7. 각 워커가 shufflePhase 실행 (병렬)
            // ============================================
            val shuffleFutures = workers.zipWithIndex.map { case (worker, myPartitionId) =>
                Future {
                    println(s"[Worker ${worker.workerId}] Shuffling partition $myPartitionId")
                    
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
            // 8. 검증
            // ============================================
            println(s"\n[Test] Verifying results...")
            
            results.foreach { case (workerId, result) =>
                assertEquals(
                    result.successCount, 
                    NUM_WORKERS,
                    s"Worker $workerId should download $NUM_WORKERS files"
                )
                
                assertEquals(
                    result.failureCount,
                    0,
                    s"Worker $workerId should have no failures"
                )
                
                val shuffleDir = testDir.resolve(s"worker_$workerId").resolve("shuffle_output")
                val downloadedFiles = Files.list(shuffleDir).count()
                
                assertEquals(
                    downloadedFiles,
                    NUM_WORKERS.toLong,
                    s"Worker $workerId should have $NUM_WORKERS files in shuffle_output"
                )
                
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
            // 9. 정리
            // ============================================
            workers.foreach(_.shutdown())
            registry.shutdown()
            
        } finally {
            deleteRecursively(testDir)
        }
    }

    test("Shuffle phase: Workers exchange partition files with gRPC (Remote)") {
        val testDir = Files.createTempDirectory("shuffle-grpc-test")
        try {
            // ============================================
            // 1. 사전 준비: 파티션 파일 생성
            // ============================================
            setupPartitionFiles(testDir)

            // ============================================
            // 2. Worker 생성
            // ============================================
            val workers = (0 until NUM_WORKERS).map { workerId =>
                val workerDir = testDir.resolve(s"worker_$workerId")
                Files.createDirectories(workerDir.resolve("shuffle_output"))

                val worker = Worker.createRemote(
                    workerId = workerId,
                    numWorkers = NUM_WORKERS,
                    workingDir = workerDir,
                    inputDirs = Seq.empty
                )

                worker
            }

            // ============================================
            // 3. Worker 시작 (gRPC 서버 시작 및 포트 수집)
            // ============================================
            val workerPorts = workers.map { worker =>
                val partitionDir = testDir.resolve(s"worker_${worker.workerId}").resolve("partitions")
                val port = worker.start(partitionDir)
                worker.workerId -> port
            }.toMap

            println(s"\n[Test] All ${NUM_WORKERS} workers started with gRPC")
            println(s"[Test] Worker ports: $workerPorts")

            // ============================================
            // 4. 워커 주소 설정 (localhost + 각 워커의 포트)
            // ============================================
            val workerAddresses = workerPorts.map { case (workerId, port) =>
                workerId -> s"localhost:$port"
            }

            workers.foreach { worker =>
                worker.setWorkerAddresses(workerAddresses)
            }

            // gRPC 서버 시작 대기
            Thread.sleep(2000)

            // ============================================
            // 5. Shuffle Phase 준비 (Remote)
            // ============================================
            workers.foreach { worker =>
                val partitionDir = testDir.resolve(s"worker_${worker.workerId}").resolve("partitions")
                worker.prepareShufflePhase(
                    useRemote = true,
                    partitionDir = partitionDir,
                    registry = None
                )
            }

            // ============================================
            // 6. FileStructure 생성
            // ============================================
            val fileStructure = createTestFileStructure(NUM_WORKERS)

            println(s"[Test] FileStructure created: ${fileStructure.allFiles.size} files")

            // ============================================
            // 7. 각 워커가 shufflePhase 실행 (병렬)
            // ============================================
            val shuffleFutures = workers.zipWithIndex.map { case (worker, myPartitionId) =>
                Future {
                    println(s"[Worker ${worker.workerId}] Shuffling partition $myPartitionId (gRPC)")

                    val result: ShuffleResult = worker.shufflePhase(
                        partitionId = myPartitionId,
                        fileStructure = fileStructure,
                        getFiles = (fs: FileStructure) => fs.getFilesForPartition(myPartitionId),
                        buildResult = (success: Int, failure: Int) => ShuffleResult(success, failure)
                    )

                    println(s"[Worker ${worker.workerId}] Shuffle complete (gRPC): " +
                            s"${result.successCount} success, ${result.failureCount} failed")

                    (worker.workerId, result)
                }
            }

            // 모든 셔플 완료 대기
            val results = Await.result(Future.sequence(shuffleFutures), 2.minutes)

            // ============================================
            // 8. 검증
            // ============================================
            println(s"\n[Test] Verifying gRPC results...")

            results.foreach { case (workerId, result) =>
                assertEquals(
                    result.successCount,
                    NUM_WORKERS,
                    s"Worker $workerId should download $NUM_WORKERS files via gRPC"
                )

                assertEquals(
                    result.failureCount,
                    0,
                    s"Worker $workerId should have no failures via gRPC"
                )

                val shuffleDir = testDir.resolve(s"worker_$workerId").resolve("shuffle_output")
                val downloadedFiles = Files.list(shuffleDir).count()

                assertEquals(
                    downloadedFiles,
                    NUM_WORKERS.toLong,
                    s"Worker $workerId should have $NUM_WORKERS files in shuffle_output (gRPC)"
                )

                (0 until NUM_WORKERS).foreach { sourceWorkerId =>
                    val fileName = s"file_${sourceWorkerId}_${workerId}_0.dat"
                    val filePath = shuffleDir.resolve(fileName)

                    assert(Files.exists(filePath), s"File $fileName should exist (gRPC)")

                    val expectedSize = RECORDS_PER_FILE * RECORD_SIZE
                    assertEquals(
                        Files.size(filePath),
                        expectedSize.toLong,
                        s"File $fileName size mismatch (gRPC)"
                    )
                }
            }

            println(s"\n[Test] ✓ All gRPC verifications passed!")

            // ============================================
            // 9. 정리
            // ============================================
            workers.foreach(_.shutdown())

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
