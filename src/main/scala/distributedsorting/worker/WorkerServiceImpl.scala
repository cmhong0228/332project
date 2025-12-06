package distributedsorting.worker

import distributedsorting.distributedsorting._
import distributedsorting.distributedsorting.{WorkerServiceGrpc, IntermediateFileRequest, IntermediateFileResponse, FileChunk}
import io.grpc.{Server, ServerBuilder}
import io.grpc.stub.StreamObserver
import java.nio.file.{Files, Path}
import java.io.{FileInputStream, BufferedInputStream}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Try, Success, Failure}
import com.typesafe.scalalogging.LazyLogging

/**
 * gRPC WorkerService 구현
 * 다른 워커로부터 파일 요청을 받아 파일 데이터를 제공
 */
class WorkerServiceImpl(
    //val workerId: Int,
    val partitionDir: Path
)(implicit ec: ExecutionContext) extends WorkerServiceGrpc.WorkerService with LazyLogging {

    private var server: Option[Server] = None
    
    private var tempWorkerId: Int = -1
    private lazy val workerId: Int = tempWorkerId

    def registerWorkerId(id: Int): Unit = {
        tempWorkerId = id
        assert(workerId >= 0)
    }

    /**
     * gRPC 서버 시작 (사용 x)
     */
    def start(): Unit = {
        val serverBuilder = ServerBuilder
            .forPort(0)
            .addService(WorkerServiceGrpc.bindService(this, ec))

        server = Some(serverBuilder.build().start())
        logger.info(s"[WorkerService $workerId] Started on port ??? ")
    }

    /**
     * gRPC 서버 종료 (사용 x)
     */
    def shutdown(): Unit = {
        server.foreach { s =>
            s.shutdown()
            s.awaitTermination()
        }
        logger.info(s"[WorkerService $workerId] Shutdown")
    }

    /**
     * GetIntermediateFile RPC 구현 (작은 파일용)
     * proto: rpc GetIntermediateFile (IntermediateFileRequest) returns (IntermediateFileResponse)
     */
    override def getIntermediateFile(request: IntermediateFileRequest): Future[IntermediateFileResponse] = {
        val fileId = FileId(request.i, request.j, request.k)
        val filePath = partitionDir.resolve(fileId.toFileName)

        logger.info(s"[WorkerService $workerId] Serving file: ${fileId.toFileName}")

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
                    logger.info(s"[WorkerService $workerId] Error serving ${fileId.toFileName}: ${ex.getMessage}")
                    IntermediateFileResponse(
                        data = com.google.protobuf.ByteString.EMPTY,
                        success = false,
                        errorMsg = ex.getMessage
                    )
            }
        }
    }

    /**
     * GetIntermediateFileStream RPC 구현 (대용량 파일용, streaming)
     * proto: rpc GetIntermediateFileStream (IntermediateFileRequest) returns (stream FileChunk)
     */
    override def getIntermediateFileStream(
        request: IntermediateFileRequest,
        responseObserver: StreamObserver[FileChunk]
    ): Unit = {
        val fileId = FileId(request.i, request.j, request.k)
        val filePath = partitionDir.resolve(fileId.toFileName)

        logger.debug(s"[WorkerService $workerId] Streaming file: ${fileId.toFileName}")

        Future {
            Try {
                if (!Files.exists(filePath)) {
                    throw new RuntimeException(s"File not found: ${fileId.toFileName}")
                }

                val fileSize = Files.size(filePath)
                val chunkSize = 1024 * 1024 // 1MB chunks
                val buffer = new Array[Byte](chunkSize)

                var inputStream: FileInputStream = null
                var bufferedStream: BufferedInputStream = null

                try {
                    inputStream = new FileInputStream(filePath.toFile)
                    bufferedStream = new BufferedInputStream(inputStream)

                    var chunkIndex = 0L
                    var bytesRead = 0
                    var totalBytesRead = 0L

                    // 파일을 청크로 나누어 전송
                    while ({ bytesRead = bufferedStream.read(buffer); bytesRead != -1 }) {
                        totalBytesRead += bytesRead

                        val isLast = totalBytesRead >= fileSize
                        val chunkData = if (bytesRead < buffer.length) {
                            buffer.take(bytesRead)
                        } else {
                            buffer
                        }

                        val chunk = FileChunk(
                            data = com.google.protobuf.ByteString.copyFrom(chunkData),
                            chunkIndex = chunkIndex,
                            totalSize = if (chunkIndex == 0) fileSize else 0,
                            isLast = isLast,
                            errorMsg = ""
                        )

                        responseObserver.onNext(chunk)
                        chunkIndex += 1
                    }

                    logger.debug(s"[WorkerService $workerId] Streamed ${fileId.toFileName}: $chunkIndex chunks, $totalBytesRead bytes")
                    responseObserver.onCompleted()

                } finally {
                    if (bufferedStream != null) bufferedStream.close()
                    if (inputStream != null) inputStream.close()
                }

            } match {
                case Success(_) => // Already handled
                case Failure(ex) =>
                    logger.info(s"[WorkerService $workerId] Error streaming ${fileId.toFileName}: ${ex.getMessage}")
                    val errorChunk = FileChunk(
                        data = com.google.protobuf.ByteString.EMPTY,
                        chunkIndex = 0,
                        totalSize = 0,
                        isLast = true,
                        errorMsg = ex.getMessage
                    )
                    responseObserver.onNext(errorChunk)
                    responseObserver.onError(new RuntimeException(ex.getMessage))
            }
        }
    }
}

/*
object WorkerServiceImpl {
    def apply(workerId: Int, partitionDir: Path)(implicit ec: ExecutionContext): WorkerServiceImpl = {
        new WorkerServiceImpl(workerId, partitionDir)
    }
}
*/