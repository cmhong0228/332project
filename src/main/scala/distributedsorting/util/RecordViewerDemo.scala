package distributedsorting.util

import distributedsorting.logic.RecordWriterRunner
import java.nio.file.{Files, Paths}
import com.typesafe.config.ConfigFactory

/**
 * RecordViewer를 테스트하기 위한 샘플 데이터 생성 프로그램
 */
object RecordViewerDemo {
  private val config = ConfigFactory.load()
  private val configPath = "distributedsorting"

  private val RECORD_LENGTH = config.getBytes(s"$configPath.record-info.record-length").toInt
  private val KEY_LENGTH = config.getBytes(s"$configPath.record-info.key-length").toInt
  private val VALUE_LENGTH = RECORD_LENGTH - KEY_LENGTH

  /**
   * 간단한 테스트 레코드 생성
   */
  def createTestRecord(keyValue: Long, message: String): Array[Byte] = {
    val record = new Array[Byte](RECORD_LENGTH)

    // Key 부분: keyValue를 바이트로 변환하여 채움
    for (i <- 0 until KEY_LENGTH) {
      record(i) = ((keyValue >> (8 * (KEY_LENGTH - 1 - i))) & 0xff).toByte
    }

    // Value 부분: 메시지를 ASCII로 채움
    val msgBytes = message.getBytes("ASCII")
    val copyLen = Math.min(msgBytes.length, VALUE_LENGTH)
    System.arraycopy(msgBytes, 0, record, KEY_LENGTH, copyLen)

    // 나머지는 공백으로 채움
    for (i <- copyLen until VALUE_LENGTH) {
      record(KEY_LENGTH + i) = ' '.toByte
    }

    record
  }

  def main(args: Array[String]): Unit = {
    val outputPath = Paths.get("test-data/demo-records.dat")

    // 디렉토리 생성
    Files.createDirectories(outputPath.getParent)

    println(s"테스트 레코드 파일 생성 중: $outputPath")

    // 10개의 테스트 레코드 생성
    val records = Seq(
      createTestRecord(0x0000000001L, "First record - smallest key"),
      createTestRecord(0x0000000010L, "Second record - key = 16"),
      createTestRecord(0x0000000100L, "Third record - key = 256"),
      createTestRecord(0x0000001000L, "Fourth record - key = 4096"),
      createTestRecord(0x0000010000L, "Fifth record - larger key"),
      createTestRecord(0x00000FFFFFL, "Sixth record - even larger"),
      createTestRecord(0x0001000000L, "Seventh record"),
      createTestRecord(0x0010000000L, "Eighth record"),
      createTestRecord(0x0100000000L, "Ninth record"),
      createTestRecord(0xFFFFFFFFFFL, "Tenth record - maximum key (10 bytes)")
    )

    RecordWriterRunner.WriteRecordIterator(outputPath, records.iterator)

    println(s"✓ 10개의 테스트 레코드가 생성되었습니다")
    println()
    println("다음 명령어로 확인하세요:")
    println(s"  ./scripts/view-records.sh $outputPath")
    println(s"  ./scripts/view-records.sh $outputPath 5")
    println(s"  ./scripts/view-records.sh $outputPath 5 --detailed")
  }
}
