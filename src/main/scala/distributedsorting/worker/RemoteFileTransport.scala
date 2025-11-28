package distributedsorting.worker

import distributedsorting.distributedsorting._
import java.nio.file.{Files, Path, StandardCopyOption}
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._
import scala.util.{Try, Success, Failure}
import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._

/**
 * gRPC를 이용한 원격 파일 전송 구현
 *
 * @param workerId 이 워커의 ID
 * @param partitionDir 이 워커의 파티션 디렉토리 (파일 제공용)
 * @param workerAddresses 다른 워커들의 주소 (workerId -> "host:port")
 */
class RemoteFileTransport(
    val workerId: Int,
    val partitionDir: Path,
    val workerAddresses: Map[Int, String]
)(implicit ec: ExecutionContext) extends FileTransport {

    private var workerService: Option[WorkerServiceImpl] = None
    private var shuffleClient: Option[ShuffleClient] = None

    val failedWorkerSet: scala.collection.mutable.Set[Int] = ConcurrentHashMap.newKeySet[Int]().asScala

    /**
     * gRPC 서버와 클라이언트 초기화
     */
    override def init(): Unit = {
        // // gRPC 서버 시작 (다른 워커로부터 파일 요청 받기)
        // val service = WorkerServiceImpl(workerId, partitionDir, port)
        // service.start()
        // workerService = Some(service)

        // gRPC 클라이언트 초기화 (다른 워커에게 파일 요청)
        val client = ShuffleClient(workerAddresses)
        shuffleClient = Some(client)

        // println(s"[RemoteFileTransport $workerId] Initialized (port: $port)")
    }

    /**
     * 다른 워커로부터 파일 요청
     * 자기 자신에게 요청하는 경우 gRPC를 거치지 않고 직접 복사
     *
     * @param fileId 요청할 파일 ID
     * @param destPath 저장할 경로
     * @return 성공 여부
     */
    override def requestFile(fileId: FileId, destPath: Path): Boolean = {
        println(s"[RemoteFileTransport $workerId] Requesting ${fileId.toFileName} from Worker ${fileId.sourceWorkerId}")

        if (Files.exists(destPath)) { // 이미 저장되어 있는 경우
            true
        } else if (failedWorkerSet.contains(fileId.sourceWorkerId)) { // 실패한 worker에 대한 재시도 하지 않음
            false
        } else if (fileId.sourceWorkerId == workerId) {
            // 자기 자신에게 요청하는 경우: gRPC 거치지 않고 직접 파일 복사
            println(s"[RemoteFileTransport $workerId] Self-request detected, copying directly")

            try {
                val sourcePath = partitionDir.resolve(fileId.toFileName)

                if (!Files.exists(sourcePath)) {
                    println(s"[RemoteFileTransport $workerId] Source file not found: ${fileId.toFileName}")
                    return false
                }

                // 파일 직접 복사
                Files.createDirectories(destPath.getParent)
                Files.copy(sourcePath, destPath, StandardCopyOption.REPLACE_EXISTING)

                val fileSize = Files.size(destPath)
                println(s"[RemoteFileTransport $workerId] Successfully copied ${fileId.toFileName} ($fileSize bytes)")
                true
            } catch {
                case e: Exception =>
                    try {
                        Files.deleteIfExists(destPath)
                    } catch {
                        case deleteEx: Exception => println("Failed to delete corrupted file")
                    }
                    println(s"[RemoteFileTransport $workerId] Failed to copy ${fileId.toFileName}: ${e.getMessage}")
                    e.printStackTrace()
                    false
            }
        } else {
            // 다른 워커에게 요청하는 경우: gRPC streaming 사용 (디스크 직접 저장)
            shuffleClient match {
                case Some(client) =>
                    try {
                        // gRPC streaming을 통해 파일을 바로 디스크에 저장
                        // 청크를 받자마자 디스크에 쓰므로 메모리 사용 최소화
                        val successFuture = client.requestFileStreamToDisk(fileId, destPath)

                        // Future를 블로킹하여 결과 대기 (최대 120초 - 대용량 파일 고려)
                        val success = Await.result(successFuture, 120.seconds)

                        if (success) {
                            val fileSize = Files.size(destPath)
                            println(s"[RemoteFileTransport $workerId] Successfully saved ${fileId.toFileName} (${fileSize} bytes)")
                        } else {
                            println(s"[RemoteFileTransport $workerId] Failed to save ${fileId.toFileName}")
                        }

                        success
                    } catch {
                        case e: Exception =>
                            try {
                                Files.deleteIfExists(destPath)
                            } catch {
                                case deleteEx: Exception => println("Failed to delete corrupted file")
                            }
                            println(s"[RemoteFileTransport $workerId] Failed to request ${fileId.toFileName}: ${e.getMessage}")
                            e.printStackTrace()
                            failedWorkerSet.add(fileId.sourceWorkerId)
                            false
                    }

                case None =>
                    println(s"[RemoteFileTransport $workerId] ShuffleClient not initialized")
                    failedWorkerSet.add(fileId.sourceWorkerId)
                    false
            }
        }
    }

    /**
     * 파일 제공 (gRPC 서버를 통해 자동 처리됨)
     *
     * RemoteFileTransport에서는 WorkerServiceImpl이 자동으로 처리하므로
     * 이 메서드는 직접 호출되지 않음
     */
    override def serveFile(fileId: FileId): Any = {
        // gRPC 서버(WorkerServiceImpl)가 자동으로 처리
        // 이 메서드는 호출되지 않음
        println(s"[RemoteFileTransport $workerId] serveFile called (handled by gRPC server)")
        ()
    }

    /**
     * gRPC 서버와 클라이언트 종료
     */
    override def close(): Unit = {
        shuffleClient.foreach(_.shutdown())
        //workerService.foreach(_.shutdown())
        println(s"[RemoteFileTransport $workerId] Closed")
    }
}

object RemoteFileTransport {
    def apply(
        workerId: Int,
        partitionDir: Path,
        workerAddresses: Map[Int, String]
    )(implicit ec: ExecutionContext): RemoteFileTransport = {
        new RemoteFileTransport(workerId, partitionDir, workerAddresses)
    }
}
