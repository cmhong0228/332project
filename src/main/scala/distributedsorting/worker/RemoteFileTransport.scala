package distributedsorting.worker

import distributedsorting.distributedsorting._
import java.nio.file.Path

// gRPC를 이용한 통신 구현
class RemoteFileTransport extends FileTransport {

    override def init(): Unit = {
        // TODO: gRPC 클라이언트 초기화
    }

    override def requestFile(fileId: FileId, destPath: Path): Boolean = {
        // TODO: ShuffleClient 호출 후 streaming방식으로 파일 받으며 destPath에 저장
        false
    }

    override def serveFile(fileId: FileId): Boolean = {
        // TODO: workerservice 호출
        false
    }
}
