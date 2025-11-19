package distributedsorting.worker

import distributedsorting.distributedsorting._
import scala.concurrent.Future

/**
 * Shuffle Phase에서 remoteFileTransport일때 다른 워커에게 파일을 요청하는 gRPC 클라이언트
 * 로컬환경 테스트 끝난 후 구현예정
 */
class ShuffleClient(workerAddresses: Map[Int, String]) {
    def requestFile(fileId: FileId): Future[Array[Byte]] = {
        // TODO: gRPC를 통해 파일 요청
        Future.failed(new NotImplementedError("ShuffleClient.requestFile not implemented"))
    }
}

object ShuffleClient {
    def apply(workerAddresses: Map[Int, String]): ShuffleClient = {
        new ShuffleClient(workerAddresses)
    }
}
