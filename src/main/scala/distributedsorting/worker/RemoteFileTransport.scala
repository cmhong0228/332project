package distributedsorting.worker

import distributedsorting.distributedsorting._
import java.nio.file.{Files, Path, StandardCopyOption}
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._
import scala.util.{Try, Success, Failure}

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

        // 자기 자신에게 요청하는 경우: gRPC 거치지 않고 직접 파일 복사
        if (fileId.sourceWorkerId == workerId) {
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
                    println(s"[RemoteFileTransport $workerId] Failed to copy ${fileId.toFileName}: ${e.getMessage}")
                    e.printStackTrace()
                    false
            }
        } else {
            // 다른 워커에게 요청하는 경우: gRPC streaming 사용
            shuffleClient match {
                case Some(client) =>
                    try {
                        // gRPC streaming을 통해 파일 데이터 요청
                        val dataFuture = client.requestFileStream(fileId)

                        // Future를 블로킹하여 결과 대기 (최대 60초 - streaming은 더 오래 걸릴 수 있음)
                        val data = Await.result(dataFuture, 60.seconds)

                        // 파일 저장
                        Files.createDirectories(destPath.getParent)
                        Files.write(destPath, data)

                        println(s"[RemoteFileTransport $workerId] Successfully saved ${fileId.toFileName} (${data.length} bytes)")
                        true
                    } catch {
                        case e: Exception =>
                            println(s"[RemoteFileTransport $workerId] Failed to request ${fileId.toFileName}: ${e.getMessage}")
                            e.printStackTrace()
                            false
                    }

                case None =>
                    println(s"[RemoteFileTransport $workerId] ShuffleClient not initialized")
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
        workerService.foreach(_.shutdown())
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
