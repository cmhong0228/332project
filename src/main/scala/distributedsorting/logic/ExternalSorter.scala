package distributedsorting.logic

import java.nio.file.Path

/**
 * 대용량 데이터를 처리하기 위한 외부 정렬(External Sorting) 기능을 정의하는 trait
 * 메모리에 한 번에 올릴 수 없는 대규모 파일 여러개를 작은 단위로 분할하여 정렬하고,
 * 이를 효율적으로 병합(k-way merge)하는 기능을 제공
 * 
 * ==============================
 * === 사용법 ===
 * ==============================
 * * 초기 setting
 * * * externalSorterInputDirectory, externalSorterOutputDirectory, externalSorterTempDirectory 경로 지정
 * * * externalSorterOrdering ordering 지정
 * * executeExternalSort 호출(파일 읽기, merge, 파일 정리 모두 수행됨)
 */
trait ExternalSorter {
    /**
     * 정렬 작업을 위해 레코드를 읽어올 입력 디렉토리의 경로
     * 이 디렉토리 내의 모든 파일이 정렬 대상
     */
    val externalSorterInputDirectory: Path

    /**
     * 최종적으로 정렬된 결과를 저장할 출력 디렉토리의 경로
     * 정렬된 단일 파일이 이 위치에 저장
     */
    val externalSorterOutputDirectory: Path

    /**
     * 정렬 과정 중 생성되는 임시 파일을 저장할 경로
     * 작업 완료 후 임시 파일들은 정리되어야 함
     */
    val externalSorterTempDirectory: Path

    /**
     * 레코드(Record) 간의 순서를 결정하는 데 사용되는 Ordering 인스턴스
     * 이 인스턴스는 레코드의 비교 로직을 정의
     */
    val externalSorterOrdering: Ordering[Record]

    /**
     * k-way merge 단계에서 한 번에 병합할 수 있는 최대 파일 또는 스트림의 개수(k 값)
     * 이 값은 시스템의 메모리 제한을 고려하여 설정
     */
    val numMaxMergeGroup: Int

    /**
     * 주어진 파일 경로 시퀀스를 최대 `numMaxMergeGroup` 크기의 그룹으로 분할
     * 이는 병합 작업을 여러 단계로 나누어 수행할 때 사용
     *
     * @param fileSeq 분할할 파일들의 `Path` 시퀀스
     * @return 최대 k개씩 묶인 `Path` 시퀀스의 시퀀스
     */
    def splitGroup(fileSeq: Seq[Path]): Seq[Seq[Path]] = ???

    /**
     * 정렬된 레코드를 담고 있는 여러 개의 Iterator를 하나의 정렬된 Iterator로 병합
     * 이 메소드는 k-way merge 알고리즘을 구현
     *
     * @param recordIters 병합할 정렬된 레코드 Iterator들의 시퀀스
     * @return 모든 입력 레코드를 정렬된 순서로 포함하는 단일 Iterator
     */
    def iteratorMerge(recordIters: Seq[Iterator[Record]]): Iterator[Record] = ???

    /**
     * 파일 경로 시퀀스를 받아서, 해당 파일들의 내용을 읽고 k-way merge를 수행하여
     * 최종적으로 정렬된 레코드 Iterator를 반환
     *
     * @param fileSeq 내용을 병합하고 정렬할 파일들의 `Path` 시퀀스
     * @return 병합된 결과를 순차적으로 제공하는 `RecordIterator`
     */
    def merge(fileSeq: Seq[Path]): Path = ???

    /**
     * 입력 디렉토리에서 정렬 대상이 되는 모든 일반 파일의 Path 시퀀스를 가져옴
     * @return 정렬 대상 파일들의 Path 시퀀스
     */
    def getInputFiles(): Seq[Path] = ???

    /**
     * 임시 디렉토리에 생성된 모든 임시 파일 및 디렉토리를 정리
     * 이 메소드는 작업의 성공/실패 여부와 관계없이 실행
     */
    def cleanUpTempFiles(): Unit = ???

    /**
     * 외부 정렬의 전체 과정을 실행하는 메인 메소드
     * 파일 목록 읽기, 정렬 수행, 임시 파일 정리까지 모두 담당
     * @return 최종 정렬 결과 파일의 Path
     */
    def executeExternalSort(): Path = ???
}