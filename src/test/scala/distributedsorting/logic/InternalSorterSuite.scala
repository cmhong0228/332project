package distributedsorting.logic

import munit.FunSuite
import java.nio.file.Path
import scala.collection.mutable.ArrayBuffer
import scala.math.Ordering
import scala.jdk.CollectionConverters._

// =======================================================
// 1. Mock 도메인 및 환경 정의 (타입 모호성 해결)
// =======================================================

// 테스트 환경에서만 사용할 Record와 Key 정의
case class TestKey(value: Long)
case class TestRecord(key: TestKey, data: Array[Byte])

// Scala 2 문법 준수 및 타입 모호성 해결을 위한 헬퍼 오브젝트
object TestDomain {
  type Record = TestRecord
  type Key = TestKey

  // 테스트용 Ordering 정의 (키 값 기반)
  implicit val TestOrdering: Ordering[TestRecord] = Ordering.by(_.key.value)
}
import TestDomain._

// [START MODIFIED: TestFileRecordIterator 오류 해결]
// Mock FileRecordIterator: 파일 읽기 시뮬레이션
class TestFileRecordIterator(filePath: Path, testData: List[TestRecord]) extends Iterator[TestRecord] {
  // FIX: 'iterator'와의 이름 충돌을 피합니다.
  private val underlyingIterator = testData.iterator
  override def hasNext: Boolean = underlyingIterator.hasNext
  override def next(): TestRecord = underlyingIterator.next()
  def close(): Unit = { /* Mock: 파일 핸들 닫기 시뮬레이션 */ }
}
// [END MODIFIED]

// Mock RecordWriterRunner: 저장 작업을 시뮬레이션하고 기록
object TestRecordWriterRunner {
  val callLogs: ArrayBuffer[(Seq[Seq[TestRecord]], List[Path])] = ArrayBuffer.empty

  def WriteRecordIterator(
                           filePath: Path,
                           recordsIterator: Iterator[TestRecord],
                           bufferSize: Int = -1
                         ): Unit = {
    // Mock: 실제 I/O 없이 저장 작업만 시뮬레이션
  }
}

// =======================================================
// 2. Test InternalSorter 구현 클래스 (InternalSorter 트레이트 가정)
// =======================================================

// InternalSorter 트레이트의 구조를 모방한 스텁 (테스트 파일 내부에 필요)
trait InternalSorter {
  val internalSorterDirectories: Seq[Path]
  val ordering : Ordering[Record]
  val filePivot : Vector[Record]
  val numOfPar : Int
  val internalSortWorkerId : Int
  val internalSorterOutputDirectory : Path
  def madeFilePath(): List[Path] = ???
  lazy val filePath: List[Path] = madeFilePath()
  lazy val filePiece: Int = filePath.size

  def madeFile(k: Int): List[Path] = ???
  def partition(sortedRecords: Seq[Record]): Seq[Seq[Record]] = ???
  def saveFile(partitionResult : Seq[Seq[Record]], outputPath : List[Path]): Unit = ???

  // runSortAndPartition 구현 (트레이트 스텁 내부에 필요)
  def runSortAndPartition(): List[Path] = {
    var allPartitionedFiles: List[Path] = ArrayBuffer.empty[Path].toList // 빈 리스트로 초기화
    for (x <- 0 until filePiece) {
      val inputPath = filePath(x)
      val fileIterator = getMockIterator(inputPath) // Mock Iterator 사용 (아래에서 정의)
      val sortedRecords: List[Record] = try {
        fileIterator.toList.sorted(ordering).toList
      } finally { fileIterator.close() }
      val outputPathsForChunk: List[Path] = madeFile(x)
      val partitionResult = partition(sortedRecords)
      saveFile(partitionResult, outputPathsForChunk)
      allPartitionedFiles = allPartitionedFiles ++ outputPathsForChunk
    }
    allPartitionedFiles
  }
  // Mocking을 위한 헬퍼 함수 (Mock Iterator를 반환하도록 TestInternalSorter에서 정의 필요)
  def getMockIterator(path: Path): TestFileRecordIterator = ???
}


class TestInternalSorter extends InternalSorter {

  // --- 추적 변수 (테스트 검증용) ---
  val madeFileCalls: ArrayBuffer[Int] = ArrayBuffer.empty
  var partitionCallCount: Int = 0
  var saveFileCallCount: Int = 0

  // --- 추상 필드 구현 (테스트 환경 설정) ---
  override val internalSorterDirectories: Seq[Path] = Seq(Path.of("in/dir1"))
  override val ordering: Ordering[Record] = TestDomain.TestOrdering
  override val filePivot: Vector[Record] = Vector(
    TestRecord(TestKey(50), Array.emptyByteArray),
    TestRecord(TestKey(100), Array.emptyByteArray)
  )
  override val numOfPar: Int = filePivot.size + 1 // 3개 파티션
  override val internalSortWorkerId: Int = 1
  override val internalSorterOutputDirectory: Path = Path.of("out/base")

  // --- madeFilePath 구현 ---
  override def madeFilePath(): List[Path] = (0 until 2).map(i => Path.of(s"input/file_$i")).toList

  // --- madeFile 구현 (핵심 검증) ---
  override def madeFile(k: Int): List[Path] = {
    madeFileCalls.append(k) // 호출 인덱스 k 기록
    (1 to numOfPar).map(j => internalSorterOutputDirectory.resolve(s"file_${internalSortWorkerId}_${j}_${k}.dat")).toList
  }

  // --- Mock Iterator 제공 ---
  override def getMockIterator(path: Path): TestFileRecordIterator = {
    path.getFileName.toString match {
      case "file_0" => new TestFileRecordIterator(path, (1 to 5).map(i => TestRecord(TestKey(55 - i * 5), Array.emptyByteArray)).toList)
      case "file_1" => new TestFileRecordIterator(path, (1 to 5).map(i => TestRecord(TestKey(110 - i * 10), Array.emptyByteArray)).toList)
      case _ => new TestFileRecordIterator(path, List.empty)
    }
  }

  // --- partition 구현 ---
  override def partition(sortedRecords: Seq[Record]): Seq[Seq[Record]] = {
    partitionCallCount += 1
    assert(sortedRecords.head.key.value <= sortedRecords.last.key.value, "입력 데이터가 정렬되지 않았습니다.")
    sortedRecords.grouped(sortedRecords.size / numOfPar + 1).toSeq
  }

  // --- saveFile 구현 ---
  override def saveFile(partitionResult: Seq[Seq[Record]], outputPath: List[Path]): Unit = {
    saveFileCallCount += 1
    assert(partitionResult.size == outputPath.size, "파티션 결과의 개수와 출력 경로의 개수가 일치해야 합니다.")
  }
}

// =======================================================
// 3. 테스트 스위트 정의
// =======================================================

class InternalSorterMunitSpec extends FunSuite {

  test("runSortAndPartition은 각 입력 파일(k)에 대해 정확히 madeFile, partition, saveFile을 호출해야 한다") {

    // 1. TestInstance 준비
    val sorter = new TestInternalSorter()

    // 2. runSortAndPartition 실행
    val resultPaths = sorter.runSortAndPartition()

    // --- 3. 최종 검증 ---
    val expectedFilePiece = 2 // madeFilePath가 반환하는 파일 수

    // a. 함수 호출 횟수 검증
    assertEquals(sorter.partitionCallCount, expectedFilePiece, "파티션 함수는 입력 파일 수만큼 호출되어야 합니다.")
    assertEquals(sorter.saveFileCallCount, expectedFilePiece, "저장 함수는 입력 파일 수만큼 호출되어야 합니다.")

    // b. 동적 경로 생성 인덱스 검증 (k 인덱스가 0, 1 순서로 전달되었는지)
    assertEquals(sorter.madeFileCalls.toSeq, Seq(0, 1), "madeFile은 루프 인덱스(k) 0, 1을 순서대로 받아야 합니다.")

    // c. 최종 반환 경로 검증
    val expectedTotalPaths = expectedFilePiece * sorter.numOfPar
    assertEquals(resultPaths.size, expectedTotalPaths, s"최종 반환된 경로는 총 $expectedTotalPaths 개여야 합니다.")
  }
}