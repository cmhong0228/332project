package distributedsorting.worker

import distributedsorting.distributedsorting._
import java.nio.file.{Files, Path, StandardCopyOption, CopyOption}
import scala.concurrent.{ExecutionContext, Future, Promise}
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean
import scala.util.{Try, Success, Failure}

/**
 * 워커 레지스트리의 최소 인터페이스
 * LocalFileTransport가 필요로 하는 메서드만 정의
 * 
 * 주의: 현재는 테스트용으로만 사용됨 (TestHelpers.WorkerRegistry 참조)
 */
trait LocalTransportRegistry {
    def register(workerId: Int, transport: LocalFileTransport): Unit
    def get(workerId: Int): Option[LocalFileTransport]
    def unregisterWorker(workerId: Int): Unit
}

// worker 하나에 대해 main thread, service thread를 분리하여
// 로컬환경에서 원격통신방식 시뮬레이션
class LocalFileTransport(
    val workerId: Int,
    val sharedDirectory: Path,
    val registry: LocalTransportRegistry  // 최소 인터페이스만 요구
)(implicit ec: ExecutionContext) extends FileTransport {

    // Service thread가 처리할 요청: (FileId, Promise[Option[Path]])
    private val requestQueue: BlockingQueue[(FileId, Promise[Option[Path]])] = 
        new LinkedBlockingQueue()
    private val running = new AtomicBoolean(false)
    private var serviceThread: Option[Thread] = None

    // 이 Worker의 파티션 디렉토리 (읽기 전용)
    private val myPartitionDir: Path = sharedDirectory
        .resolve(s"worker_$workerId")
        .resolve("partitions")
    
    // 서비스 스레드 init
    override def init(): Unit = {
        running.set(true)
        
        // Service Thread 시작
        val thread = new Thread(() => {
            println(s"[Service Thread $workerId] Started")
        
            while (running.get()) {
                try {
                    val request = requestQueue.poll(100, TimeUnit.MILLISECONDS)
                    
                    if (request != null) {
                        val (fileId, promise) = request
                        // Service thread가 파일 경로 제공
                        val result = serveFile(fileId)
                        promise.success(result)
                    }
                } catch {
                    case _: InterruptedException => 
                        Thread.currentThread().interrupt()
                    case e: Exception =>
                        println(s"[Service Thread $workerId] Error: ${e.getMessage}")
                }
            }
            
            println(s"[Service Thread $workerId] Stopped")
        })
        thread.setName(s"ServiceThread-Worker-$workerId")
        thread.setDaemon(true)
        thread.start()
        
        // Registry에 워커 등록
        registry.register(workerId, this)
        serviceThread = Some(thread)
    }

     override def requestFile(fileId: FileId, destPath: Path): Boolean = {
        import scala.concurrent.Await
        import scala.concurrent.duration._
        println(s"[Worker $workerId] Requesting file from Worker ${fileId.sourceWorkerId} " +
            s"(thread: ${Thread.currentThread().getName})")
    
        // 내부적으로 비동기 처리 후 결과를 블로킹하여 Boolean으로 변환
        val resultFuture: Future[Try[Unit]] = if (fileId.sourceWorkerId == workerId) {
            // 자기 자신에게 요청하는 경우: Service thread 거치지 않고 직접 처리
            println(s"[Worker $workerId] Self-request detected, copying directly")
            serveFile(fileId) match {
                case Some(sourcePath) =>
                    Future {
                        try {
                            Files.copy(
                                sourcePath,
                                destPath,
                                StandardCopyOption.REPLACE_EXISTING
                            )
                            Success(()): Try[Unit]
                        } catch {
                            case e: Exception => Failure(e): Try[Unit]
                        }
                    }
                case None =>
                    Future.successful(Failure(
                        new RuntimeException(s"File not found: ${fileId.toFileName}")
                    ): Try[Unit])
            }
        } else {
            // 다른 워커에게 요청하는 경우: Service thread 통해서 처리
            registry.get(fileId.sourceWorkerId) match {
                case Some(targetTransport) =>
                    val promise = Promise[Option[Path]]()
                    
                    // 대상 워커의 Service thread 큐에 요청 추가
                    targetTransport.enqueueRequest(fileId, promise)
                    
                    // Service thread로부터 응답(파일 경로) 대기 후 파일 복사
                    promise.future.map { pathOpt: Option[Path] =>
                        pathOpt match {
                            case Some(sourcePath) =>
                                try {
                                    Files.copy(
                                        sourcePath,
                                        destPath,
                                        StandardCopyOption.REPLACE_EXISTING
                                    )
                                    Success(()): Try[Unit]
                                } catch {
                                    case e: Exception => Failure(e): Try[Unit]
                                }
                            case None =>
                                Failure(new RuntimeException(s"File not found: ${fileId.toFileName}")): Try[Unit]
                        }
                    }.recover {
                        case e: Exception => Failure(e): Try[Unit]
                    }
                    
                case None =>
                    Future.successful(Failure(
                        new RuntimeException(s"Worker ${fileId.sourceWorkerId} not found in registry")
                    ): Try[Unit])
            }
        }

        // Future를 블로킹하여 Boolean으로 변환
        try {
            val result = Await.result(resultFuture, 30.seconds)
            result.isSuccess
        } catch {
            case e: Exception =>
                println(s"[Worker $workerId] Error requesting file ${fileId.toFileName}: ${e.getMessage}")
                false
        }
    }
    
    /**
     * 요청을 큐에 추가 (다른 워커의 main thread가 호출)
     */
    def enqueueRequest(fileId: FileId, promise: Promise[Option[Path]]): Unit = {
        requestQueue.offer((fileId, promise))
    }

    /**
     * 파일 제공 (Service thread 또는 자기 자신의 main thread에서 호출)
     */
    def serveFile(fileId: FileId): Option[Path] = {
        val filePath: Path = myPartitionDir.resolve(fileId.toFileName)
    
        if (Files.exists(filePath)) {
            println(s"[Worker $workerId] Serving ${fileId.toFileName} " +
                s"(thread: ${Thread.currentThread().getName})")
            Some(filePath)
        } else {
            None
        }
    }

    override def close(): Unit = {
        if (running.compareAndSet(true, false)) {
            serviceThread.foreach { thread =>
                thread.interrupt()
                thread.join(5000)
            }
            registry.unregisterWorker(workerId)
        }
    }
}