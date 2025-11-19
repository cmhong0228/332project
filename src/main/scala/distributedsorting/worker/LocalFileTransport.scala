package distributedsorting.worker

import distributedsorting.distributedsorting._
import java.nio.file.Path

/**
 * 워커 레지스트리의 최소 인터페이스
 * LocalFileTransport가 필요로 하는 메서드만 정의
 * 
 * 주의: 현재는 테스트용으로만 사용됨 (TestHelpers.WorkerRegistry 참조)
 */
trait LocalTransportRegistry {
    def register(workerId: Int, transport: LocalFileTransport): Unit
    def get(workerId: Int): Option[LocalFileTransport]
}

// worker 하나에 대해 main thread, service thread를 분리하여
// 로컬환경에서 원격통신방식 시뮬레이션
class LocalFileTransport(
    val workerId: Int,
    val sharedDirectory: Path,
    val registry: LocalTransportRegistry  // 최소 인터페이스만 요구
) extends FileTransport {
    
    //서비스스레드 init
    override def init(): Unit = {
        // Service Thread 시작
        val thread = new Thread(() => {
            println(s"[LocalFileTransport $workerId] Service thread started")
            // TODO: 서비스 스레드 로직
        })
        thread.setDaemon(true)
        thread.start()
        
        // Registry에 워커 등록
        registry.register(workerId, this)
    }

    override def requestFile(fileId: FileId, destPath: Path): Boolean = {
        // TODO: 서비스 스레드에 요청 후 destPath에 저장
        false
    }

    def serveFile(fileId: FileId, sourcePath: Path): Boolean = {
        // TODO: 서비스 스레드에서 파일 제공
        // 워커디렉토리에서 공유디렉토리에 파일 업로드하는 방식으로 제공 
        // 또는 remote방식을 최대한 모사하기위해 streaming 등의 방식으로 제공
        false
    }
}
