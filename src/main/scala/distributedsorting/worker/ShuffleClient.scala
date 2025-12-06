package distributedsorting.worker

import distributedsorting.distributedsorting._
import distributedsorting.distributedsorting.{WorkerServiceGrpc, IntermediateFileRequest, FileChunk}
import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import io.grpc.stub.StreamObserver
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ArrayBuffer
import com.typesafe.scalalogging.LazyLogging

/**
 * Shuffle Phase에서 다른 워커에게 파일을 요청하는 gRPC 클라이언트
 */
class ShuffleClient(
    workerAddresses: Map[Int, String]
)(implicit ec: ExecutionContext) extends LazyLogging {

    // 각 워커에 대한 gRPC 채널 캐시
    private val channels = TrieMap[Int, ManagedChannel]()
    private val stubs = TrieMap[Int, WorkerServiceGrpc.WorkerServiceStub]()

    /**
     * 워커 ID에 대한 gRPC stub 가져오기 (lazy initialization)
     */
    private def getStub(workerId: Int): WorkerServiceGrpc.WorkerServiceStub = {
        stubs.getOrElseUpdate(workerId, {
            val address = workerAddresses.getOrElse(
                workerId,
                throw new RuntimeException(s"Worker $workerId address not found")
            )

            // address format: "host:port"
            val Array(host, port) = address.split(":")

            val channel = ManagedChannelBuilder
                .forAddress(host, port.toInt)
                .usePlaintext()
                .build()

            channels.put(workerId, channel)

            WorkerServiceGrpc.stub(channel)
        })
    }

    /**
     * 파일 요청 (작은 파일용 - 단일 응답)
     *
     * @param fileId 요청할 파일 ID
     * @return 파일 데이터 (Future)
     */
    def requestFile(fileId: FileId): Future[Array[Byte]] = {
        val stub = getStub(fileId.sourceWorkerId)

        val request = IntermediateFileRequest(
            i = fileId.sourceWorkerId,
            j = fileId.partitionId,
            k = fileId.sortedRunId
        )

        logger.debug(s"[ShuffleClient] Requesting ${fileId.toFileName} from Worker ${fileId.sourceWorkerId}")

        stub.getIntermediateFile(request).map { response =>
            if (response.success) {
                logger.debug(s"[ShuffleClient] Received ${fileId.toFileName} (${response.data.size()} bytes)")
                response.data.toByteArray
            } else {
                throw new RuntimeException(s"Failed to get file ${fileId.toFileName}: ${response.errorMsg}")
            }
        }
    }

    /**
     * 파일 요청 (대용량 파일용 - streaming, 메모리 효율적)
     *
     * 청크를 받자마자 바로 디스크에 저장하여 메모리 사용 최소화
     *
     * @param fileId 요청할 파일 ID
     * @param destPath 저장할 파일 경로
     * @return 성공 여부 (Future)
     */
    def requestFileStreamToDisk(fileId: FileId, destPath: java.nio.file.Path): Future[Boolean] = {
        import java.io.{FileOutputStream, BufferedOutputStream}
        import java.nio.file.Files

        val stub = getStub(fileId.sourceWorkerId)

        val request = IntermediateFileRequest(
            i = fileId.sourceWorkerId,
            j = fileId.partitionId,
            k = fileId.sortedRunId
        )

        logger.debug(s"[ShuffleClient] Requesting ${fileId.toFileName} from Worker ${fileId.sourceWorkerId} (streaming to disk)")

        val promise = Promise[Boolean]()
        var totalSize = 0L
        var receivedBytes = 0L
        var outputStream: BufferedOutputStream = null

        val responseObserver = new StreamObserver[FileChunk] {
            override def onNext(chunk: FileChunk): Unit = {
                try {
                    if (chunk.errorMsg.nonEmpty) {
                        promise.failure(new RuntimeException(s"Error receiving chunk: ${chunk.errorMsg}"))
                        if (outputStream != null) {
                            outputStream.close()
                        }
                        return
                    }

                    // 첫 청크에서 파일 스트림 열기
                    if (chunk.chunkIndex == 0) {
                        totalSize = if (chunk.totalSize > 0) chunk.totalSize else 0L
                        logger.debug(s"[ShuffleClient] Receiving ${fileId.toFileName}: $totalSize bytes")

                        Files.createDirectories(destPath.getParent)
                        outputStream = new BufferedOutputStream(
                            new FileOutputStream(destPath.toFile)
                        )
                    }

                    // 청크를 바로 디스크에 저장 (메모리에 누적하지 않음!)
                    val chunkData = chunk.data.toByteArray
                    if (outputStream != null) {
                        outputStream.write(chunkData)
                        receivedBytes += chunkData.length
                    }

                    if (chunk.chunkIndex % 100 == 0) {
                        logger.debug(s"[ShuffleClient] Progress ${fileId.toFileName}: chunk ${chunk.chunkIndex}, ${receivedBytes/1024/1024}MB received")
                    }

                    // 마지막 청크면 파일 닫기
                    if (chunk.isLast) {
                        if (outputStream != null) {
                            outputStream.flush()
                            outputStream.close()
                            outputStream = null
                        }
                        logger.debug(s"[ShuffleClient] ✓ Completed ${fileId.toFileName}: $receivedBytes bytes")
                        promise.success(true)
                    }
                } catch {
                    case e: Exception =>
                        logger.error(s"[ShuffleClient] Error writing chunk: ${e.getMessage}", e)
                        if (outputStream != null) {
                            try { outputStream.close() } catch { case _: Exception => }
                        }
                        promise.failure(e)
                }
            }

            override def onError(t: Throwable): Unit = {
                logger.error(s"[ShuffleClient] Error streaming ${fileId.toFileName}: ${t.getMessage}", t)
                if (outputStream != null) {
                    try { outputStream.close() } catch { case _: Exception => }
                }
                promise.failure(t)
            }

            override def onCompleted(): Unit = {
                // onNext에서 이미 처리됨
                if (!promise.isCompleted) {
                    logger.debug(s"[ShuffleClient] Stream completed for ${fileId.toFileName}")
                    if (outputStream != null) {
                        try {
                            outputStream.flush()
                            outputStream.close()
                        } catch { case _: Exception => }
                    }
                }
            }
        }

        stub.getIntermediateFileStream(request, responseObserver)

        promise.future
    }

    /**
     * 파일 요청 (대용량 파일용 - streaming, 레거시 메모리 버퍼링 방식)
     *
     * 주의: 모든 청크를 메모리에 누적하므로 큰 파일에는 비효율적
     * requestFileStreamToDisk 사용 권장
     *
     * @param fileId 요청할 파일 ID
     * @return 파일 데이터 (Future)
     */
    @deprecated("Use requestFileStreamToDisk for better memory efficiency", "1.0")
    def requestFileStream(fileId: FileId): Future[Array[Byte]] = {
        val stub = getStub(fileId.sourceWorkerId)

        val request = IntermediateFileRequest(
            i = fileId.sourceWorkerId,
            j = fileId.partitionId,
            k = fileId.sortedRunId
        )

        logger.debug(s"[ShuffleClient] Requesting ${fileId.toFileName} from Worker ${fileId.sourceWorkerId} (streaming - memory buffering)")

        val promise = Promise[Array[Byte]]()
        val chunks = ArrayBuffer[Array[Byte]]()
        var totalSize = 0L
        var receivedBytes = 0L

        val responseObserver = new StreamObserver[FileChunk] {
            override def onNext(chunk: FileChunk): Unit = {
                if (chunk.errorMsg.nonEmpty) {
                    promise.failure(new RuntimeException(s"Error receiving chunk: ${chunk.errorMsg}"))
                    return
                }

                // 첫 청크에서 전체 파일 크기 저장
                if (chunk.chunkIndex == 0 && chunk.totalSize > 0) {
                    totalSize = chunk.totalSize
                    logger.debug(s"[ShuffleClient] Receiving ${fileId.toFileName}: $totalSize bytes")
                }

                // 청크 데이터 저장
                val chunkData = chunk.data.toByteArray
                chunks += chunkData
                receivedBytes += chunkData.length

                logger.debug(s"[ShuffleClient] Received chunk ${chunk.chunkIndex} of ${fileId.toFileName}: ${chunkData.length} bytes")

                // 마지막 청크면 데이터 조합
                if (chunk.isLast) {
                    val allData = chunks.flatten.toArray
                    logger.debug(s"[ShuffleClient] Completed receiving ${fileId.toFileName}: ${allData.length} bytes (${chunks.size} chunks)")
                    promise.success(allData)
                }
            }

            override def onError(t: Throwable): Unit = {
                logger.error(s"[ShuffleClient] Error streaming ${fileId.toFileName}: ${t.getMessage}", t)
                promise.failure(t)
            }

            override def onCompleted(): Unit = {
                // onNext에서 이미 처리됨
                if (!promise.isCompleted) {
                    logger.debug(s"[ShuffleClient] Stream completed for ${fileId.toFileName}")
                }
            }
        }

        stub.getIntermediateFileStream(request, responseObserver)

        promise.future
    }

    /**
     * 모든 채널 종료
     */
    def shutdown(): Unit = {
        channels.values.foreach { channel =>
            channel.shutdown()
        }
        channels.clear()
        stubs.clear()
        logger.debug(s"[ShuffleClient] Shutdown complete")
    }
}

object ShuffleClient {
    def apply(workerAddresses: Map[Int, String])(implicit ec: ExecutionContext): ShuffleClient = {
        new ShuffleClient(workerAddresses)
    }
}
