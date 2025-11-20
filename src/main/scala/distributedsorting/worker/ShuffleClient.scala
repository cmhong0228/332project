package distributedsorting.worker

import distributedsorting.distributedsorting._
import distributedsorting.distributedsorting.{WorkerServiceGrpc, IntermediateFileRequest}
import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import scala.concurrent.{ExecutionContext, Future}
import scala.collection.concurrent.TrieMap

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
     * 파일 요청
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
