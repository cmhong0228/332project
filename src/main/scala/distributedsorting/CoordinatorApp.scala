package distributedsorting

import java.nio.file.{Files, Path, Paths}
import scala.collection.mutable

/**
 * 워커들을 조율하는 코디네이터
 * 사용법: sbt "runMain distributedsorting.CoordinatorApp 3"
 * 
 * 변경사항:
 * - 각 워커의 포트를 자동으로 수집
 * - 워커 주소 정보를 각 워커에게 전달
 */
object CoordinatorApp extends App {
  
  if (args.length < 1) {
    println("Usage: CoordinatorApp <numWorkers>")
    println("Example: CoordinatorApp 3")
    System.exit(1)
  }
  
  val numWorkers = args(0).toInt
  
  println(s"=" * 60)
  println(s"Shuffle Test Coordinator")
  println(s"  Workers: $numWorkers")
  println(s"=" * 60)
  
  val baseDir = Paths.get(System.getProperty("user.dir"), "test-data", "multi-process")
  
  // 1. 기존 데이터 정리
  println("\n[Coordinator] Step 1: Cleaning up old data...")
  if (Files.exists(baseDir)) {
    deleteRecursively(baseDir)
  }
  Files.createDirectories(baseDir)
  
  println("\n[Coordinator] ✓ Setup complete!")
  println("\n" + "=" * 60)
  println("Now start workers in separate terminals:")
  println("=" * 60)
  (0 until numWorkers).foreach { workerId =>
    println(s"Terminal ${workerId + 1}: sbt \"runMain distributedsorting.WorkerApp $workerId $numWorkers\"")
  }
  println("=" * 60)
  println("\nNote: Ports are now auto-allocated (no need to specify)")
  
  // 2. 모든 워커가 준비될 때까지 대기
  println("\n[Coordinator] Step 2: Waiting for all workers to be ready...")
  var ready = Set.empty[Int]
  while (ready.size < numWorkers) {
    (0 until numWorkers).foreach { workerId =>
      if (!ready.contains(workerId)) {
        val readyFile = baseDir.resolve(s"ready_worker_$workerId")
        if (Files.exists(readyFile)) {
          ready += workerId
          println(s"[Coordinator] ✓ Worker $workerId is ready (${ready.size}/$numWorkers)")
        }
      }
    }
    Thread.sleep(500)
  }
  
  println("\n[Coordinator] ✓ All workers are ready!")
  
  // 3. 모든 워커의 포트 정보 수집
  println("\n[Coordinator] Step 3: Collecting worker port information...")
  val workerPorts = mutable.Map[Int, Int]()
  
  (0 until numWorkers).foreach { workerId =>
    val portFile = baseDir.resolve(s"port_worker_$workerId.txt")
    
    var retries = 0
    while (!Files.exists(portFile) && retries < 20) {
      Thread.sleep(500)
      retries += 1
    }
    
    if (!Files.exists(portFile)) {
      println(s"[Coordinator] ✗ Failed to get port info for worker $workerId")
      System.exit(1)
    }
    
    val port = new String(Files.readAllBytes(portFile)).trim.toInt
    workerPorts(workerId) = port
    println(s"[Coordinator] Worker $workerId -> localhost:$port")
  }
  
  // 4. 워커 주소 정보를 각 워커에게 전달
  println("\n[Coordinator] Step 4: Distributing worker addresses...")
  val workerAddresses = workerPorts.map { case (id, port) =>
    s"$id=localhost:$port"
  }.mkString("\n")
  
  (0 until numWorkers).foreach { workerId =>
    val addressFile = baseDir.resolve(s"addresses_worker_$workerId.txt")
    Files.write(addressFile, workerAddresses.getBytes)
    println(s"[Coordinator] ✓ Sent addresses to worker $workerId")
  }
  
  println("\n[Coordinator] ✓ All workers have address information!")
  println("[Coordinator] Press Enter to start shuffle phase...")
  scala.io.StdIn.readLine()
  
  // 5. 모든 워커에 시작 신호 보내기
  println("\n[Coordinator] Step 5: Sending start signals to workers...")
  (0 until numWorkers).foreach { workerId =>
    val signalFile = baseDir.resolve(s"start_worker_$workerId")
    Files.createFile(signalFile)
    println(s"[Coordinator] ✓ Signaled worker $workerId")
  }
  
  println("\n[Coordinator] Waiting for workers to complete shuffle...")
  
  // 6. 모든 워커의 완료 대기
  var completed = Set.empty[Int]
  val startTime = System.currentTimeMillis()
  
  while (completed.size < numWorkers) {
    (0 until numWorkers).foreach { workerId =>
      if (!completed.contains(workerId)) {
        val doneFile = baseDir.resolve(s"done_worker_$workerId")
        if (Files.exists(doneFile)) {
          completed += workerId
          val elapsed = (System.currentTimeMillis() - startTime) / 1000.0
          println(s"[Coordinator] ✓ Worker $workerId completed (${completed.size}/$numWorkers) - ${elapsed}s elapsed")
        }
      }
    }
    Thread.sleep(500)
  }
  
  val totalTime = (System.currentTimeMillis() - startTime) / 1000.0
  
  println("\n" + "=" * 60)
  println(s"[Coordinator] All workers completed shuffle in ${totalTime}s!")
  println("=" * 60)
  
  // 7. 결과 검증
  println("\n[Coordinator] Step 6: Verifying results...")
  verifyResults(baseDir, numWorkers)
  
  println("\n[Coordinator] ✓ Test completed successfully!")
  println("[Coordinator] You can now stop the workers (Ctrl+C in each terminal)")
  
  def verifyResults(baseDir: Path, numWorkers: Int): Unit = {
    val RECORDS_PER_FILE = 1000
    val RECORD_SIZE = 100
    val FILES_PER_PARTITION = 3  // WorkerApp과 동일하게 설정
    val expectedSize = RECORDS_PER_FILE * RECORD_SIZE
    val expectedFilesPerWorker = numWorkers * FILES_PER_PARTITION  // 각 워커가 받아야 할 총 파일 수
    
    var allPassed = true
    
    println("\n" + "-" * 60)
    println("Verification Results:")
    println("-" * 60)
    
    (0 until numWorkers).foreach { workerId =>
      val shuffleDir = baseDir.resolve(s"worker_$workerId").resolve("shuffle_output")
      
      if (!Files.exists(shuffleDir)) {
        println(s"[Worker $workerId] ✗ shuffle_output directory not found")
        allPassed = false
        return
      }
      
      val fileCount = Files.list(shuffleDir).count()
      if (fileCount != expectedFilesPerWorker) {
        println(s"[Worker $workerId] ✗ Expected $expectedFilesPerWorker files, found $fileCount")
        allPassed = false
      } else {
        // 파일 크기 검증
        var fileSizeOk = true
        var verifiedCount = 0
        
        // 각 source worker로부터
        (0 until numWorkers).foreach { sourceWorkerId =>
          // 각 index 파일에 대해
          (0 until FILES_PER_PARTITION).foreach { fileIndex =>
            val fileName = s"file_${sourceWorkerId}_${workerId}_${fileIndex}.dat"
            val filePath = shuffleDir.resolve(fileName)
            
            if (!Files.exists(filePath)) {
              println(s"[Worker $workerId] ✗ Missing file $fileName")
              allPassed = false
              fileSizeOk = false
            } else {
              val actualSize = Files.size(filePath)
              if (actualSize != expectedSize) {
                println(s"[Worker $workerId] ✗ File $fileName size mismatch: expected $expectedSize, got $actualSize")
                allPassed = false
                fileSizeOk = false
              } else {
                verifiedCount += 1
              }
            }
          }
        }
        
        if (fileSizeOk) {
          println(s"[Worker $workerId] ✓ All $verifiedCount files verified (${expectedSize} bytes each)")
        }
      }
    }
    
    println("-" * 60)
    
    if (allPassed) {
      val totalFiles = numWorkers * numWorkers * FILES_PER_PARTITION
      val totalDataKB = totalFiles * expectedSize / 1024
      val totalDataMB = totalDataKB / 1024.0
      
      println(s"\n🎉 SUCCESS: All verifications passed! 🎉")
      println(s"Files per partition: $FILES_PER_PARTITION")
      println(s"Total files transferred: $totalFiles")
      println(s"Total data transferred: $totalDataKB KB (${totalDataMB} MB)")
    } else {
      println(s"\n❌ FAILURE: Some verifications failed!")
    }
  }
  
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
