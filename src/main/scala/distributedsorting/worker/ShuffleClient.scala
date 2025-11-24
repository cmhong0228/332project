package distributedsorting.worker

import distributedsorting.distributedsorting._
import distributedsorting.distributedsorting.{WorkerServiceGrpc, IntermediateFileRequest, FileChunk}
import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import io.grpc.stub.StreamObserver
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ArrayBuffer

/**
 * Shuffle Phase에서 다른 워커에게 파일을 요청하는 gRPC 클라이언트
 */
class ShuffleClient(
    workerAddresses: Map[Int, String]
)(implicit ec: ExecutionContext) {

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

        println(s"[ShuffleClient] Requesting ${fileId.toFileName} from Worker ${fileId.sourceWorkerId}")

        stub.getIntermediateFile(request).map { response =>
            if (response.success) {
                println(s"[ShuffleClient] Received ${fileId.toFileName} (${response.data.size()} bytes)")
                response.data.toByteArray
            } else {
                throw new RuntimeException(s"Failed to get file ${fileId.toFileName}: ${response.errorMsg}")
            }
        }
    }

    /**
     * 파일 요청 (대용량 파일용 - streaming)
     *
     * @param fileId 요청할 파일 ID
     * @return 파일 데이터 (Future)
     */
    def requestFileStream(fileId: FileId): Future[Array[Byte]] = {
        val stub = getStub(fileId.sourceWorkerId)

        val request = IntermediateFileRequest(
            i = fileId.sourceWorkerId,
            j = fileId.partitionId,
            k = fileId.sortedRunId
        )

        println(s"[ShuffleClient] Requesting ${fileId.toFileName} from Worker ${fileId.sourceWorkerId} (streaming)")

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
                    println(s"[ShuffleClient] Receiving ${fileId.toFileName}: $totalSize bytes")
                }

                // 청크 데이터 저장
                val chunkData = chunk.data.toByteArray
                chunks += chunkData
                receivedBytes += chunkData.length

                println(s"[ShuffleClient] Received chunk ${chunk.chunkIndex} of ${fileId.toFileName}: ${chunkData.length} bytes")

                // 마지막 청크면 데이터 조합
                if (chunk.isLast) {
                    val allData = chunks.flatten.toArray
                    println(s"[ShuffleClient] Completed receiving ${fileId.toFileName}: ${allData.length} bytes (${chunks.size} chunks)")
                    promise.success(allData)
                }
            }

            override def onError(t: Throwable): Unit = {
                println(s"[ShuffleClient] Error streaming ${fileId.toFileName}: ${t.getMessage}")
                promise.failure(t)
            }

            override def onCompleted(): Unit = {
                // onNext에서 이미 처리됨
                if (!promise.isCompleted) {
                    println(s"[ShuffleClient] Stream completed for ${fileId.toFileName}")
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
        println(s"[ShuffleClient] Shutdown complete")
    }
}

object ShuffleClient {
    def apply(workerAddresses: Map[Int, String])(implicit ec: ExecutionContext): ShuffleClient = {
        new ShuffleClient(workerAddresses)
    }
}
