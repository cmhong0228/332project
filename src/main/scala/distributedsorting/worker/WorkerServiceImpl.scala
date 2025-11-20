package distributedsorting.worker

import distributedsorting.distributedsorting._
import distributedsorting.distributedsorting.{WorkerServiceGrpc, IntermediateFileRequest, IntermediateFileResponse}
import io.grpc.{Server, ServerBuilder}
import java.nio.file.{Files, Path}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Try, Success, Failure}

/**
 * gRPC WorkerService 구현
 * 다른 워커로부터 파일 요청을 받아 파일 데이터를 제공
 */
class WorkerServiceImpl(
    val workerId: Int,
    val partitionDir: Path,
    val port: Int
)(implicit ec: ExecutionContext) extends WorkerServiceGrpc.WorkerService {

    private var server: Option[Server] = None

    /**
     * gRPC 서버 시작
     */
    def start(): Unit = {
        val serverBuilder = ServerBuilder
            .forPort(port)
            .addService(WorkerServiceGrpc.bindService(this, ec))

        server = Some(serverBuilder.build().start())
        println(s"[WorkerService $workerId] Started on port $port")
    }

    /**
     * gRPC 서버 종료
     */
    def shutdown(): Unit = {
        server.foreach { s =>
            s.shutdown()
            s.awaitTermination()
        }
        println(s"[WorkerService $workerId] Shutdown")
    }

    /**
     * GetIntermediateFile RPC 구현
     * proto: rpc GetIntermediateFile (IntermediateFileRequest) returns (IntermediateFileResponse)
     */
    override def getIntermediateFile(request: IntermediateFileRequest): Future[IntermediateFileResponse] = {
        val fileId = FileId(request.i, request.j, request.k)
        val filePath = partitionDir.resolve(fileId.toFileName)

        println(s"[WorkerService $workerId] Serving file: ${fileId.toFileName}")

        Future {
            Try {
                if (!Files.exists(filePath)) {
                    throw new RuntimeException(s"File not found: ${fileId.toFileName}")
                }

                val fileData = Files.readAllBytes(filePath)

                IntermediateFileResponse(
                    data = com.google.protobuf.ByteString.copyFrom(fileData),
                    success = true,
                    errorMsg = ""
                )
            } match {
                case Success(response) => response
                case Failure(ex) =>
                    println(s"[WorkerService $workerId] Error serving ${fileId.toFileName}: ${ex.getMessage}")
                    IntermediateFileResponse(
                        data = com.google.protobuf.ByteString.EMPTY,
                        success = false,
                        errorMsg = ex.getMessage
                    )
            }
        }
    }
}

object WorkerServiceImpl {
    def apply(workerId: Int, partitionDir: Path, port: Int)(implicit ec: ExecutionContext): WorkerServiceImpl = {
        new WorkerServiceImpl(workerId, partitionDir, port)
    }
}
