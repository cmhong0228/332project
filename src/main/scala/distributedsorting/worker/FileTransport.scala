package distributedsorting.worker

import distributedsorting.distributedsorting._
import java.nio.file.Path

// 통신관련 추상화trait
// 통신방식을 encapsulation하여 shufflePhase에서 FileTransport 추상메서드만 사용하도록 구현
// (이후 다른 Phase로도 확장가능)

trait FileTransport {
    def init(): Unit
    def requestFile(fileId: FileId, destPath: Path): Boolean
    def serveFile(fileId: FileId): Any
    def close(): Any
}
