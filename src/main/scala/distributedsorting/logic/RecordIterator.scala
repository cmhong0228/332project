package distributedsorting.logic

import distributedsorting.distributedsorting._
import java.io.{InputStream, BufferedInputStream, EOFException, FileInputStream, IOException}
import java.nio.file.Path

/**
 * stream의 내용을 Recore Iterator로 반환하는 클래스
 *
 * =======================================================
 * === 사용 ===
 * =======================================================
 * * 생성자에 inputStream과 recordSize를 입력으로 받음
 * * Iterator와 사용방법 동일
 * * 사용 완료한 해당 iterator는 close()로 리소스 정리 필수
 * * close는 무조건 RecordIterator로 생성된 원본 객체에서 호출, concat등 iterator연산으로 생성된 객체에서 호출 불가
 *
 * =======================================================
 * === 사용 예시 (Usage Example) ===
 * =======================================================
 * * 인스턴스 생성
 *      val iterator = new StreamRecordIterator(inputStream, 8192)
 * * for 루프를 사용한 모든 레코드 소비:
 *      try {
 *      for (record <- iterator) {
 *          // record: 100바이트 Array[Byte]
 *          ...
 *      }
 *      } finally {
 *          iterator.close() // 리소스 정리 필수
 *      }
 *
 * @param inputStream 레코드를 읽어올 입력 스트림
 * @param recordSize 레코드의 고정 크기(byte)
 */
class StreamRecordIterator(
    inputStream: InputStream, 
    recordSize: Int
) extends Iterator[Record] with AutoCloseable {
    require(recordSize > 0, "recordSize must be a positive integer.")

    /**
     * 룩어헤드 임시 저장소
     * 다음에 next() 호출 시 반환할 레코드를 미리 로드하여 이터레이터의 상태를 유지
     * (레코드 존재 시 Some(Record), 파일 끝 도달 시 None)
     */
    private var nextRecord: Option[Record] = None 
    
    /**
     * 디스크에서 정확히 RECORD_SIZE만큼의 바이트를 읽어와 Record 객체를 로드
     * 파일 끝(-1)이나 불완전한 레코드 감지 시 None을 반환하며, 
     * I/O 예외 발생 시 스트림을 닫고 RuntimeException을 던짐
     */
    private def loadNext(): Option[Record] = {
        if (isClosed) {
            return None
        }

        try {
            val buffer = new Array[Byte](recordSize)
            var totalBytesRead = 0
            var bytesRead = 0

            while (totalBytesRead < recordSize && bytesRead != -1) {
                // buffer의 'totalBytesRead' 위치부터, 'recordSize - totalBytesRead' 만큼 읽기를 시도
                bytesRead = inputStream.read(buffer, totalBytesRead, recordSize - totalBytesRead)
                
                if (bytesRead != -1) {
                    totalBytesRead += bytesRead
                }
            }

            if (totalBytesRead == recordSize) {
                Some(buffer)
            } else if (totalBytesRead == 0 && bytesRead == -1) {
                None
            } else {
                // 불완전한 레코드
                None
            }
            
        } catch {
            case e: IOException =>
                try {
                    close()
                } catch {
                    case ce: IOException => e.addSuppressed(ce)
                }
                throw new RuntimeException("Failed to read next record from stream", e)
        }
    }

    /**
     * 스트림에 다음에 소비할 레코드가 있는지 확인
     * nextRecord 필드의 상태(isDefined)를 빠르게 확인하여 I/O 없이 응답
     *
     * @return 다음 레코드가 존재하면 true
     */
    override def hasNext: Boolean = nextRecord.isDefined

    /**
     * 다음 Record 를 반환하고,
     * loadNext()를 호출하여 이터레이터를 다음 위치로 전진
     *
     * @return 다음 레코드
     * @throws NoSuchElementException 더 이상 읽을 레코드가 없을 때 호출되면 발생
     */
    override def next(): Record = {
        if (!hasNext) {
            throw new NoSuchElementException("next() called on empty iterator")
        }
        val currentRecord = nextRecord.get
        nextRecord = loadNext()
        currentRecord
    }

    var isClosed: Boolean = false
    /**
     * 사용 완료 후 파일 핸들 및 내부 I/O 리소스를 닫아 해제
     * 리소스 누수를 방지하기 위해 반드시 호출되어야 함
     */
    override def close(): Unit = {
        if (!isClosed) {
            try {
                inputStream.close()
            } finally {
                isClosed = true
                nextRecord = None 
            }
        }
    }

    nextRecord = loadNext()
}


/**
 * RecordIterator Wrapper 클래스: 파일 경로를 받는 사용자 인터페이스. (I/O 및 자원 관리 책임)
 * StreamRecordIterator에 I/O 책임을 위임
 *
 * IO 효율성을 높이기 위해 java.io.BufferedInputStream 사용하여 버퍼링 수행
 * 버퍼 사용으로 I/O 사용을 최소화함
 *
 * =======================================================
 * === 사용 ===
 * =======================================================
 * * 생성자에 filePath를 넘겨주어 사용, recordSize, bufferSize는 옵션
 * * Iterator와 사용방법 동일
 * * 사용 완료한 해당 iterator는 close()로 리소스 정리 필수
 * * close는 무조건 RecordIterator로 생성된 원본 객체에서 호출, concat등 iterator연산으로 생성된 객체에서 호출 불가
 *
 * =======================================================
 * === 사용 예시 (Usage Example) ===
 * =======================================================
 * * 1. Conf 파일 설정을 사용하여 인스턴스 생성 및 순회 (권장):
 *      val iterator = new FileRecordIterator(Paths.get("/data/input/block1"))
 * * 2. 수동으로 레코드 크기/버퍼 크기를 지정하여 순회:
 *      // Record 크기 128, Buffer 크기 32KB 지정
 *      val iterator = new FileRecordIterator(Paths.get("/data/input/block2"), 128, 32768)
 * * 3. for 루프를 사용한 모든 레코드 소비:
 *      try {
 *      for (record <- iterator) {
 *          // record: 100바이트 Array[Byte]
 *          ...
 *      }
 *      } finally {
 *          iterator.close() // 리소스 정리 필수
 *      }
 *
 * @param filePath 읽을 데이터 파일의 경로
 * @param recordSize 읽을 record의 크기(byte)(0, 음수 플래그 시 conf에서 로드)
 * @param bufferSize 사용할 buffer의 크기(byte)(0, 음수 플래그 시 conf에서 로드)
 */
class FileRecordIterator(
    filePath: Path, 
    recordSize: Int = -1, 
    bufferSize: Int = -1
) extends Iterator[Record] with AutoCloseable {

    /**
     * 레코드의 고정 길이 (바이트).
     * 생성자 인자(recordSize)가 음수(-1)인 경우, 
     * 설정 파일(application.conf)에서 값을 읽어와 설정
     */
    private[logic] val RECORD_SIZE: Int = ???
    
    /**
     * inputStream이 사용할 buffer size (바이트).
     * 생성자 인자(bufferSize)가 음수(-1)인 경우, 
     * 설정 파일(application.conf)에서 값을 읽어와 설정
     */
    private[logic] val BUFFER_SIZE: Int = ???

    private val inputStream: BufferedInputStream = ???
    
    // 핵심 로직을 StreamRecordIterator에 위임 (Delegation)
    private val delegate: StreamRecordIterator = 
        new StreamRecordIterator(inputStream, RECORD_SIZE)

    // Iterator 메소드 위임
    override def hasNext: Boolean = delegate.hasNext
    override def next(): Record = delegate.next()

    /**
     * close() 메소드 위임
     * FileRecordIterator 인스턴스가 닫힐 때, 내부의 StreamRecordIterator를 통해 스트림을 닫음
     * 사용 완료 후 파일 핸들 및 내부 I/O 리소스를 닫아 해제
     * 리소스 누수를 방지하기 위해 반드시 호출되어야 함
     */
    override def close(): Unit = delegate.close()
}