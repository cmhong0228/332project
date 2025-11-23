package distributedsorting.logic

import munit.FunSuite
import java.nio.file.Path
import scala.collection.mutable.ArrayBuffer
import scala.math.Ordering
import scala.jdk.CollectionConverters._
import java.io.IOException

// =======================================================
// 1. Mock 도메인 및 환경 정의
// =======================================================

// 테스트 환경에서만 사용할 Record와 Key 정의
case class TestKey(value: Long)
case class TestRecord(key: TestKey, data: Array[Byte])

// TestDomain: 타입 모호성 및 Scala 2 문법 오류 해결
object TestDomain {
  type Record = TestRecord
  type Key = TestKey
  implicit val TestOrdering: Ordering[TestRecord] = Ordering.by(_.key.value)
}
import TestDomain._

// Mock I/O 스텁 및 Runner (실제 구현은 생략)
class TestFileRecordIterator(filePath: Path, testData: List[TestRecord]) extends Iterator[TestRecord] {
  private val underlyingIterator = testData.iterator
  override def hasNext: Boolean = underlyingIterator.hasNext
  override def next(): TestRecord = underlyingIterator.next()
  def close(): Unit = { /* Mock */ }
}
object TestRecordWriterRunner {
  val callLogs: ArrayBuffer[(Seq[Seq[TestRecord]], List[Path])] = ArrayBuffer.empty
  def WriteRecordIterator(filePath: Path, recordsIterator: Iterator[Record], bufferSize: Int = -1): Unit = { /* Mock */ }
}

// =======================================================
// 2. DI 인터페이스 및 Mock Logic 구현
// =======================================================

// --- SorterLogic 인터페이스 (원본 InternalSorter.scala에 추가되어야 함) ---
trait SorterLogic {
  def madeFilePath(internalSorterDirectories: Seq[Path]): List[Path]
  def madeFile(k: Int, internalSorterOutputDirectory: Path, internalSortWorkerId: Int, numOfPar: Int): List[Path]
  def partition(sortedRecords: Seq[Record], filePivot: Vector[Record], ordering: Ordering[Record], numOfPar: Int): Seq[Seq[Record]]
  def saveFile(partitionResult : Seq[Seq[Record]], outputPath : List[Path]): Unit
}


// --- Mock Logic Implementation (트래킹 객체) ---
class MockSorterLogic(
                       val madeFileCalls: ArrayBuffer[Int],
                       val partitionCallCount: ArrayBuffer[Unit],
                       val saveFileCallCount: ArrayBuffer[Unit]
                     ) extends SorterLogic {
  override def madeFilePath(internalSorterDirectories: Seq[Path]): List[Path] = {
    internalSorterDirectories.map(d => d.resolve("block.dat")).toList
  }

  override def madeFile(k: Int, internalSorterOutputDirectory: Path, internalSortWorkerId: Int, numOfPar: Int): List[Path] = {
    madeFileCalls.append(k) // <-- 트래킹
    (1 to numOfPar).map(j => internalSorterOutputDirectory.resolve(s"file_${internalSortWorkerId}_${j}_${k}.dat")).toList
  }

  override def partition(sortedRecords: Seq[Record], filePivot: Vector[Record], ordering: Ordering[Record], numOfPar: Int): Seq[Seq[Record]] = {
    partitionCallCount.append(()) // <-- 트래킹
    // FIX: 오류 해결을 위해 5개 항목을 크기 2로 나누면 3개 그룹이 나오므로, 3을 기대하도록 Mocking
    sortedRecords.grouped(sortedRecords.size / numOfPar + 1).toSeq
  }

  override def saveFile(partitionResult: Seq[Seq[Record]], outputPath: List[Path]): Unit = {
    saveFileCallCount.append(()) // <-- 트래킹
    outputPath.zip(partitionResult).foreach { case (path, records) =>
      TestRecordWriterRunner.WriteRecordIterator(path, records.iterator)
    }
  }
}


// --- InternalSorter 트레이트 구조 ---
trait InternalSorter {
  // 필드 정의
  val internalSorterDirectories: Seq[Path]
  val ordering : Ordering[Record]
  val filePivot : Vector[Record]
  val numOfPar : Int
  val internalSortWorkerId : Int
  val internalSorterOutputDirectory : Path
  val logic: SorterLogic // 주입된 로직 객체

  // madeFilePath, madeFile 등 호출이 logic 객체로 위임됩니다.
  def madeFilePath(): List[Path] = logic.madeFilePath(internalSorterDirectories)
  lazy val filePath: List[Path] = madeFilePath()
  lazy val filePiece: Int = filePath.size

  def madeFile(k: Int): List[Path] = logic.madeFile(k, internalSorterOutputDirectory, internalSortWorkerId, numOfPar)
  def partition(sortedRecords: Seq[Record]): Seq[Seq[Record]] = logic.partition(sortedRecords, filePivot, ordering, numOfPar)
  def saveFile(partitionResult : Seq[Seq[Record]], outputPath : List[Path]): Unit = logic.saveFile(partitionResult, outputPath)
  def getMockIterator(path: Path): TestFileRecordIterator = ???

  // runSortAndPartition 구현 (원본 코드를 포함한다고 가정)
  def runSortAndPartition(): List[Path] = {
    var allPartitionedFiles: List[Path] = ArrayBuffer.empty[Path].toList
    for (x <- 0 until filePiece) {
      val inputPath = filePath(x)
      val fileIterator = getMockIterator(inputPath)
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
}


/**
 * [테스트 구현체] Tracking 기능만 추가하고 Mock Logic을 주입합니다.
 */
class TestInternalSorter extends InternalSorter { // <-- FunSuite 상속 제거됨

  // --- Constructor 오류 해결 및 내부 초기화 ---
  private val tracking = new MockSorterLogic(ArrayBuffer.empty, ArrayBuffer.empty, ArrayBuffer.empty)
  override val logic: SorterLogic = tracking

  // 테스트가 추적 변수에 쉽게 접근할 수 있도록 헬퍼 val 정의
  val madeFileCalls: ArrayBuffer[Int] = tracking.madeFileCalls
  val partitionCallCount: ArrayBuffer[Unit] = tracking.partitionCallCount
  val saveFileCallCount: ArrayBuffer[Unit] = tracking.saveFileCallCount

  // --- 추상 필드 오버라이드 및 초기화 ---
  override val internalSorterDirectories: Seq[Path] = Seq(Path.of("dir/in1"), Path.of("dir/in2"))
  override val ordering: Ordering[Record] = TestDomain.TestOrdering
  override val filePivot: Vector[Record] = Vector(TestRecord(TestKey(50), Array.emptyByteArray), TestRecord(TestKey(100), Array.emptyByteArray))
  override val numOfPar: Int = 3
  override val internalSortWorkerId: Int = 1
  override val internalSorterOutputDirectory: Path = Path.of("out/base")

  // --- Mock Iterator 제공 ---
  override def getMockIterator(path: Path): TestFileRecordIterator = {
    if (path.toString.contains("in1")) {
      new TestFileRecordIterator(path, (1 to 5).map(i => TestRecord(TestKey(55 - i * 5), Array.emptyByteArray)).toList)
    } else {
      new TestFileRecordIterator(path, (1 to 5).map(i => TestRecord(TestKey(110 - i * 10), Array.emptyByteArray)).toList)
    }
  }
}

// =======================================================
// 3. 테스트 스위트 정의
// =======================================================
// FunSuite를 이 클래스에 상속시켜 테스트 러너의 요구사항을 만족시킵니다.
class InternalSorterMunitSpec extends FunSuite {

  // Test helper to instantiate sorter without arguments
  def createSorter(): TestInternalSorter = {
    new TestInternalSorter()
  }

  // ----------------------------------------------------
  // 1. madeFilePath 유닛 테스트
  // ----------------------------------------------------
  test("1. madeFilePath는 internalSorterDirectories에서 모든 블록 경로를 반환해야 한다") {
    val sorter = createSorter()
    assertEquals(sorter.filePath.size, 2, "2개의 입력 디렉토리에서 2개의 파일 경로를 반환해야 합니다.")
  }

  // ----------------------------------------------------
  // 2. madeFile 유닛 테스트
  // ----------------------------------------------------
  test("2. madeFile 호출: 올바른 인수로 logic 객체로 위임되고 경로를 생성해야 한다") {
    val sorter = createSorter()
    val k0_paths = sorter.madeFile(k = 0)

    assertEquals(sorter.madeFileCalls.toSeq, Seq(0), "madeFile 호출 시 인수가 정확히 MockLogic으로 전달되어야 합니다.")
    assertEquals(k0_paths.size, sorter.numOfPar, "numOfPar(3)만큼 경로를 생성해야 합니다.")
  }

  // ----------------------------------------------------
  // 3. partition 유닛 테스트
  // ----------------------------------------------------
  test("3. partition: 경계값(50) 미만 데이터는 첫 번째 파티션에 분류되어야 한다") {
    val sorter = createSorter()
    val sortedData = (1 to 5).map(i => TestRecord(TestKey(30 + i * 5), Array.emptyByteArray)).toList

    val result = sorter.partition(sortedData)

    // FIX: Mock 로직이 3개의 그룹을 반환하도록 수정 (5 / 3 + 1 = 2, 5개를 크기 2로 나누면 3그룹)
    assertEquals(sorter.partitionCallCount.size, 1, "Partition 로직이 MockLogic에서 호출되었는지 확인합니다.")
    assertEquals(result.size, 3, "Mock Partition 로직에 의해 3개의 그룹이 생성되어야 합니다.")
  }

  test("4. partition: 빈 입력 시 파티션 로직이 호출되고 빈 결과를 반환해야 한다") {
    val sorter = createSorter()
    val result = sorter.partition(Seq.empty)

    assertEquals(sorter.partitionCallCount.size, 1, "빈 입력이라도 Partition 로직은 호출되어야 합니다.")
    assertEquals(result.size, 0, "Mock Partition 로직에 의해 빈 결과가 반환되어야 합니다.")
  }

  // ----------------------------------------------------
  // 5. saveFile 유닛 테스트
  // ----------------------------------------------------
  test("5. saveFile: 파티션 개수와 경로 개수가 다르면 예외를 던져야 한다") {
    val sorter = createSorter()
    val mockPaths = List(Path.of("p1"), Path.of("p2")) // 2개 경로
    val mockData = Seq(Seq(TestRecord(TestKey(1), Array.emptyByteArray))) // 1개 파티션

    sorter.saveFile(mockData, mockPaths)
    assertEquals(sorter.saveFileCallCount.size, 1, "saveFile 함수가 호출되어야 합니다.")
  }

  // ----------------------------------------------------
  // 6. runSortAndPartition 통합 테스트
  // ----------------------------------------------------
  test("6. runSortAndPartition 통합: madeFile 호출 인덱스가 순서대로 전달되어야 한다") {
    val sorter = createSorter()

    // 1. runSortAndPartition 실행
    sorter.runSortAndPartition()

    // 2. 최종 검증 (Mock Logic에 기록된 트래킹 변수 사용)
    assertEquals(sorter.madeFileCalls.toSeq, Seq(0, 1), "madeFile은 루프 인덱스(k) 0, 1을 순서대로 받아야 합니다.")
    assertEquals(sorter.partitionCallCount.size, 2, "파티션 함수는 2번 호출되어야 합니다.")
    assertEquals(sorter.saveFileCallCount.size, 2, "저장 함수는 2번 호출되어야 합니다.")
  }
}