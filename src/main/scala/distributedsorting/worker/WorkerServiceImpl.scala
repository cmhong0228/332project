package distributedsorting.worker

import distributedsorting.distributedsorting._
import scala.concurrent.Future

class WorkerServiceImpl {

    /**
     * 다른 워커의 shuffle client가 파일 요청을 하면,
     * 현재 워커의 Map O/D에서 streaming식으로 보내는 gRPC workerservice
     */
    def serveFile(fileId: FileId): Future[Array[Byte]] = {
        // TODO: gRPC 서비스 구현
        Future.failed(new NotImplementedError("WorkerServiceImpl.serveFile not implemented"))
    }
}

object WorkerServiceImpl {
    def apply(): WorkerServiceImpl = new WorkerServiceImpl()
}
