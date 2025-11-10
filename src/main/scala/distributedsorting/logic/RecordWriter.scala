package distributedsorting.logic

import distributedsorting.distributedsorting._
import java.io.{OutputStream, BufferedOutputStream, FileOutputStream, IOException}
import java.nio.file.{Files, Path}
import com.typesafe.config.ConfigFactory

/**
 * Record Iterator를 파일 시스템 경로에 쓰는 클래스
 *
 * =======================================================
 * === 사용 및 주의 사항 ===
 * =======================================================
 * **인스턴스 생성:** 출력 파일 경로와 버퍼 크기(옵션)를 넘겨 인스턴스를 생성
 * **리소스 정리 필수:** 반드시 **try-finally** 구문 내에서 close()를 호출하여 
 * 파일 핸들을 해제하고 버퍼 데이터를 디스크에 강제 저장(flush)해야 함
 * **호출:** writeAll에 record iterator를 넘겨 사용
 *
 * =======================================================
 * === 사용 예시 (Usage Example) ===
 * =======================================================
 * * val writer = new RecordWriter(outputStream)
 * * try {
 * *    writer.writeAll(sortedIterator) // Iterator를 넘겨 스트리밍 시작
 * * } finally {
 * *    writer.close() // 버퍼 내용 디스크 반영 및 리소스 정리
 * * }
 *
 * @param outputStream 쓸 데이터 stream
 */
class RecordWriter(outputStream: OutputStream) {     

    /**
     * 레코드 이터레이터를 받아, 이터레이터가 끝날 때까지 
     * 레코드를 하나씩 스트리밍 방식으로 출력 파일에 씀
     * **[주의]** 이 메서드는 입력 Iterator의 close()를 대신 호출하지 않음
     * 입력 Iterator의 리소스는 호출자가 별도로 해제해야 함
     * * @param recordsIterator 쓸 레코드의 Iterator
     */
    def writeAll(recordsIterator: Iterator[Record]): Unit = {
        if (isClosed) {
            throw new IOException("Cannot writeAll to a closed RecordWriter")
        }

        try {
            recordsIterator.foreach(outputStream.write)
        } catch {
            case e: IOException => 
                try {
                    close()
                } catch {
                    case ce: IOException => e.addSuppressed(ce)
                }
                throw new RuntimeException("Failed during record writing", e)
        }
    }
    
    private var isClosed = false
    /**
     * 사용 완료 후 파일 핸들 및 내부 I/O 리소스를 닫아 해제
     * 리소스 누수를 방지하기 위해 반드시 호출되어야 함
     */
    def close(): Unit = {
        if (!isClosed) {
            try {
                outputStream.flush()
                outputStream.close()
            } finally {
                isClosed = true
            }
        }
    }
}

/**
 * RecordWriter 클래스의 인스턴스 생명주기(생성-사용-종료)를 관리하는 유틸리티 객체
 * * 이 객체는 RecordWriter를 사용하는 호출자가 매번 try-finally 구문을 작성하고 
 * close() 호출을 잊지 않도록 강제하는 역할 수행
 * 리소스 누수(Resource Leak)를 방지
 */
object RecordWriterRunner {
    private final val config = ConfigFactory.load()
    private final val configPath = "distributedsorting"
    /**
     * outputStream, RecordWriter를 자동으로 생성, 사용(writeAll 호출), 그리고 닫는 래퍼 함수.
     * * 이 함수는 RecordWriter 인스턴스를 생성하고, 주어진 Iterator를 사용하여 
     * 쓰기 작업을 실행한 후, 작업 성공 또는 실패 여부와 관계없이 
     * 반드시 close()를 호출하여 리소스를 정리
     *
     * === 주의 사항 ===
     * * **입력 Iterator 해제:** 이 함수는 출력 스트림(RecordWriter)만 닫음 
     * recordsIterator가 파일 읽기 리소스(RecordIterator)에서 온 경우, 
     * 해당 recordsIterator에 대한 close()는 **호출자가 별도로** 처리해야 함
     *
     * @param filePath 출력 파일 경로. RecordWriter 인스턴스 생성에 사용
     * @param recordsIterator 파일에 쓸 레코드의 Iterator. writeAll 함수에 전달
     * @param bufferSize RecordWriter의 버퍼 크기 옵션
     */
    def WriteRecordIterator(
        filePath: Path, 
        recordsIterator: Iterator[Record], 
        bufferSize: Int = -1
    ): Unit = {
        val BUFFER_SIZE = {
            if (bufferSize > 0) bufferSize
            else config.getBytes(s"$configPath.io.buffer-size").toInt
        }

        var writer: RecordWriter = null

        try {
            val fileStream: OutputStream = Files.newOutputStream(filePath)
            
            val bufferStream: BufferedOutputStream = new BufferedOutputStream(fileStream, BUFFER_SIZE)
            
            writer = new RecordWriter(bufferStream)
            
            writer.writeAll(recordsIterator)

        } catch {
            case e: Exception =>
                throw new RuntimeException(s"Failed to write records to $filePath", e)
        } finally {
            if (writer != null) {
                writer.close()
            }
        }
    }
}