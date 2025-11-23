package distributedsorting.logic

import java.nio.file.Path
import scala.util.{Try, Success, Failure}
import scala.jdk.CollectionConverters._

/**
 * 12~16 - 타입 불일치 문제로 추가 (distributedSorting.Record와 Record를 컴파일러가 이 둘이 동일한데 다르게 해석)
 * 128줄 오류 해결 위해 추가
 */

import distributedsorting.distributedsorting.Record

// --- Core Trait ---

trait InternalSorter {
  /**
   * input directory들의 Seq.
   */
  val internalSorterDirectories: Seq[Path]

  val ordering : Ordering[Record]

  val filePivot : Vector[Record]


  val numOfPar : Int

  val internalSortWorkerId : Int

  val internalSorterOutputDirectory : Path


  // --- 유틸리티 함수 정의 ---
  /**
   * 주어지는 internalSorterDirectories에 있는 파일들의 경로를 리스트에 저장
   * @return inputDirectory의 파일들의 경로를 저장한 List[Path]를 반환
   */
  def madeFilePath(): List[Path] = {
    // TODO: internalSorterDirectories를 순회하며 파일 경로를 수집하는 로직 구현
    ???
  }

  /**
   * 파티션 개수 만큼의 Path 생성
   * 각 파일들의 이름은 file_i_j_k.dat (i: InternalSorterWorkerId, j: 각 partition index(1~numOfPar), k: File index)
   * @param k - 각 파티션에 할당될 파일의 index
   * @return 생성된 파일 경로 List[Path]를 반환
   */
  def madeFile(k: Int): List[Path] = {
    // TODO: numOfPar, internalSortWorkerId, filePiece 등의 필드를 사용하여
    //       요구되는 파일명 패턴(file_i_j_k.dat)에 맞는 경로를 생성하는 로직 구현
    ???
  }


  // Used lazy val to avoid initialization order dependency on InternalSorterDirectories
  // madeFilePath 호출 시 인자 없음 (트레이트 필드 사용)
  lazy val filePath: List[Path] = madeFilePath()


  lazy val filePiece: Int = filePath.size


  // =======================================================
  // 배치(Batch) 기반 파티션/저장 함수
  // =======================================================

  /**
   * TODO
   * partition(각 데이터를 pivot의 범위와 순차적으로 비교하면서 이에 해당하는 값을 시퀸스에 저장)
   * pivot에 따라서 시퀸스를 나누어 저장(numOfPar 길이의 시퀸스)
   * 범위가 없는 경우에는 빈 시퀸스로 둠
   * @param sortedRecords - sorting 단계를 거쳐서 정렬된 Record
   * @return Record들을 pivot의 범위에 따라 분류한 Seq[Seq[Record]] (배치 결과)
   */
  def partition(sortedRecords: Seq[Record]): Seq[Seq[Record]] = {
    // TODO: sortedRecords를 filePivot과 ordering을 사용하여 numOfPar 크기의 시퀀스로 분할하는 로직 구현
    ???
  }

  /**
   * TODO
   * partition 후 반환된 Seq[Seq[Record]]를 각 해당하는 파일에 저장
   * output directory에 저장하는 부분은 RecordWriter 활용
   * Partition 증가 index 그대로
   * @param partitionResult - partition의 반환값 (분류된 레코드 시퀀스)
   * @param outputPath - 각 File들을 저장할 output directory의 경로
   */
  def saveFile(partitionResult : Seq[Seq[Record]], outputPath : List[Path]): Unit = {
    // TODO: partitionResult의 각 시퀀스를 RecordWriterRunner를 활용하여 해당 outputPath[i]에 저장하는 로직 구현
    ???
  }


  /**
   * 주 실행 로직: 입력 파일을 정렬하고, 배치 함수를 호출합니다.
   */
  def runSortAndPartition(): List[Path] = {

    // 각 파티션별 최종 파일 경로 목록 (saveFile의 outputPath 인수로 전달될 값)
    // madeFile(numOfPar, internalSorterOutputDirectory)가 되어야 하지만,
    // 파일 index(k)를 계산해야 하므로 구체적인 구현 클래스에서 처리해야 함.

    var allPartitionedFiles: List[Path] = List.empty

    for (x <- 0 until filePiece) {
      val inputPath = filePath(x)

      // 1. Initialize Iterator safely
      // FileRecordIterator는 같은 패키지에 정의되어 있으므로 사용 가능
      val fileIterator = new FileRecordIterator(inputPath)

      // 2. Read, ToList, and Sort within a try-finally for resource safety
      val sortedRecords: List[Record] = try {

        val newSort = fileIterator.toList.sorted(ordering)

        // Record는 다른 scala 파일에서 만들어진 디렉터리의 데이터
        // 주어진 파일경로(recordIterator in record-io)에서 이터레이터로 바꾼 다음에 .toList로 리스트화
        newSort
      } finally {
        // Must close the iterator to release file handles
        fileIterator.close()
      }

      val outputPaths: List[Path] = madeFile(x)

      // 3. 정렬된 레코드를 배치 partition 함수에 전달
      val partitionResult = partition(sortedRecords)

      // 4. 파티션 결과를 파일에 저장
      saveFile(partitionResult, outputPaths)

      /**
       * TODO
       * 파티션 버퍼에 남아있는 잔여 데이터를 saveFile()을 호출하여 모두 디스크에 저장하고,
       * 저장된 파일 경로 리스트를 반환
       */
      // saveFile이 Unit을 반환하므로, 여기서는 outputPaths를 반환하는 방식으로 처리
      allPartitionedFiles = outputPaths
    }
    allPartitionedFiles
  }
}