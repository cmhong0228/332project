package distributedsorting.util

import distributedsorting.distributedsorting._
import distributedsorting.logic.FileRecordIterator
import java.nio.file.{Path, Paths}
import com.typesafe.config.ConfigFactory
import scala.util.Using

/**
 * 바이너리 레코드 파일(.dat)을 읽어서 사람이 읽을 수 있는 형식으로 출력하는 유틸리티
 *
 * 사용법:
 *   sbt "runMain distributedsorting.util.RecordViewer <파일경로> [레코드개수]"
 *
 * 예시:
 *   sbt "runMain distributedsorting.util.RecordViewer /output/file_0_0_0.dat 10"
 *   sbt "runMain distributedsorting.util.RecordViewer test-data/input/data.dat"
 */
object RecordViewer {
  private val config = ConfigFactory.load()
  private val configPath = "distributedsorting"

  private val RECORD_LENGTH = config.getBytes(s"$configPath.record-info.record-length").toInt
  private val KEY_LENGTH = config.getBytes(s"$configPath.record-info.key-length").toInt
  private val VALUE_LENGTH = RECORD_LENGTH - KEY_LENGTH

  /**
   * byte 배열을 16진수 문자열로 변환
   */
  def bytesToHex(bytes: Array[Byte]): String = {
    bytes.map(b => f"${b & 0xff}%02x").mkString
  }

  /**
   * byte 배열을 ASCII 문자열로 변환 (출력 가능한 문자만)
   * 출력 불가능한 문자는 '.'로 표시
   */
  def bytesToAscii(bytes: Array[Byte]): String = {
    bytes.map { b =>
      val char = (b & 0xff).toChar
      if (char >= 32 && char < 127) char else '.'
    }.mkString
  }

  /**
   * 레코드를 사람이 읽을 수 있는 형식으로 출력
   */
  def printRecord(index: Int, record: Record): Unit = {
    require(record.length == RECORD_LENGTH, s"Invalid record length: ${record.length}")

    val key = record.slice(0, KEY_LENGTH)
    val value = record.slice(KEY_LENGTH, RECORD_LENGTH)

    println(f"[$index%6d] Key: ${bytesToHex(key)} | Value: ${bytesToAscii(value)}")
  }

  /**
   * 레코드를 더 상세한 형식으로 출력 (16진수 + ASCII 모두 표시)
   */
  def printRecordDetailed(index: Int, record: Record): Unit = {
    require(record.length == RECORD_LENGTH, s"Invalid record length: ${record.length}")

    val key = record.slice(0, KEY_LENGTH)
    val value = record.slice(KEY_LENGTH, RECORD_LENGTH)

    println(s"[$index] =====================================")
    println(s"  Key (hex):   ${bytesToHex(key)}")
    println(s"  Key (ascii): ${bytesToAscii(key)}")
    println(s"  Value (hex):   ${bytesToHex(value)}")
    println(s"  Value (ascii): ${bytesToAscii(value)}")
  }

  /**
   * 파일의 레코드를 읽어서 출력
   *
   * @param filePath 읽을 파일 경로
   * @param maxRecords 출력할 최대 레코드 수 (None이면 전체)
   * @param detailed 상세 모드 여부
   */
  def viewRecords(filePath: Path, maxRecords: Option[Int] = None, detailed: Boolean = false): Unit = {
    println(s"파일: $filePath")
    println(s"레코드 크기: $RECORD_LENGTH bytes (Key: $KEY_LENGTH, Value: $VALUE_LENGTH)")
    println("=" * 80)

    Using.resource(new FileRecordIterator(filePath)) { iterator =>
      var count = 0
      var totalCount = 0

      while (iterator.hasNext && maxRecords.forall(count < _)) {
        val record = iterator.next()

        if (detailed) {
          printRecordDetailed(count, record)
        } else {
          printRecord(count, record)
        }

        count += 1
        totalCount += 1
      }

      // 남은 레코드 개수 세기
      while (iterator.hasNext) {
        iterator.next()
        totalCount += 1
      }

      println("=" * 80)
      println(s"출력된 레코드: $count")
      println(s"전체 레코드: $totalCount")
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.isEmpty) {
      println("사용법: RecordViewer <파일경로> [최대레코드수] [--detailed]")
      println()
      println("예시:")
      println("  RecordViewer /output/file_0_0_0.dat")
      println("  RecordViewer /output/file_0_0_0.dat 10")
      println("  RecordViewer /output/file_0_0_0.dat 10 --detailed")
      System.exit(1)
    }

    val filePath = Paths.get(args(0))
    val maxRecords = if (args.length > 1 && args(1) != "--detailed") {
      Some(args(1).toInt)
    } else {
      None
    }
    val detailed = args.contains("--detailed")

    try {
      viewRecords(filePath, maxRecords, detailed)
    } catch {
      case e: Exception =>
        println(s"에러 발생: ${e.getMessage}")
        e.printStackTrace()
        System.exit(1)
    }
  }
}
