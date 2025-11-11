package distributedsorting.logic

import distributedsorting.distributedsorting._
import java.nio.file.{Files, Path}
import scala.util.Random
import scala.jdk.CollectionConverters._
import java.io.IOException
import distributedsorting.logic.FileRecordIterator
import scala.math.min

/**
 * Sampler 트레이트는 주어진 확률에 따라 레코드 스트림에서 Key를 추출하는 순수 샘플링 알고리즘의 책임을 정의
 */
trait Sampler {
    // key의 길이
    val KEY_SIZE: Int

    /**
     * 주어진 확률로 입력 Iterator에서 Key를 샘플링
     * @param inputIterator 입력 레코드 스트림
     * @param samplingRatio 마스터로부터 받은 샘플링 확률 (k/n)
     * @return 샘플링된 Key 리스트 (Seq[Key])
     */
    def sampleKeys(inputIterator: Iterator[Record], samplingRatio: Double): Seq[Key] = {
        val rand = new Random()

        val sampledKeysIterator = inputIterator.collect {
            case record if rand.nextDouble() < samplingRatio =>
                assert(record.length >= KEY_SIZE)
                record.take(KEY_SIZE)
        }

        sampledKeysIterator.toSeq
    }
}

/**
 * DataSizeCalculator 트레이트는 디스크 I/O 관련 책임을 정의
 * Worker 노드에 할당된 전체 데이터의 개수를 계산하는 역할을 수행
 */
trait RecordCountCalculator {
    // record의 길이
    val RECORD_SIZE: Int

    /**
     * Worker 노드에 할당된 입력 디렉터리 경로 목록을 기반으로 전체 데이터의 총 레코드 개수를 계산
     * 이 값은 마스터(Master)에게 보고되어 샘플링 비율 결정 시 모집단 크기(numTotalRecords)로 사용
     * @param inputDirs Worker가 처리할 데이터가 있는 디스크 경로 목록
     * @return 총 레코드 개수 (Long 타입)
     */
    def calculateTotalRecords(inputDirs: Seq[Path]): Long = {
        if (inputDirs.isEmpty || RECORD_SIZE <= 0) {
            return 0L
        }
        val totalSize = inputDirs.map { dirPath =>
            try {
                if (Files.isDirectory(dirPath)) {
                    val stream = Files.walk(dirPath, 1) 
                    try {
                        stream
                            .iterator().asScala // Java Stream을 Scala Iterator로 변환
                            .filter(path => path != dirPath) // 디렉터리 자신은 제외
                            .filter(Files.isRegularFile(_)) // 파일만 필터링
                            .map(Files.size(_)) // 각 파일의 크기(Long)를 가져옴
                            .sum // 이 디렉터리의 모든 파일 크기를 합산
                    } finally {
                        stream.close()
                    }
                } else {
                    0L // 경로가 디렉터리가 아니면 0으로 처리
                }
            } catch {
                case e: IOException =>
                    0L
            }
        }.sum // 모든 디렉터리의 총 파일 크기를 합산

        totalSize / RECORD_SIZE
    }
}

/**
 * RecordExtractor 트레이트는 디스크 I/O를 수행하여 데이터를 읽고, 
 * 필요한 비율만큼 레코드를 추출하여 Key만 반환하는 책임을 정의
 * 실제 sample 과정은 Sampler에서 진행
 */
trait RecordExtractor { self: Sampler =>
    // key의 길이
    val KEY_SIZE: Int

    /**
     * 입력 디렉터리 경로에서 데이터를 읽고, 주어진 샘플링 비율을 고려하여 레코드를 추출한 뒤 Key 리스트를 반환
     * @param inputDirs 데이터를 읽어올 디스크 경로 목록
     * @param samplingRatio Master에게 받은 최종 샘플링 확률
     * @return 추출된 Key 객체의 리스트 (Seq[Key])
     */
    def readAndExtractSamples(inputDirs: Seq[Path], samplingRatio: Double): Seq[Key] = {
        val allFilePaths: Seq[Path] = inputDirs.flatMap { dirPath =>
            try {
                if (Files.isDirectory(dirPath)) {
                    val stream = Files.walk(dirPath, 1) // 1단계 깊이만 탐색
                    try {
                        stream.iterator().asScala
                            .filter(path => path != dirPath && Files.isRegularFile(path))
                            .toSeq // 스트림을 닫기 위해 즉시 Seq로 변환
                    } finally {
                        stream.close()
                    }
                } else {
                    Seq.empty[Path] // 디렉터리가 아니면 무시
                }
            } catch {
                case e: IOException =>
                    Seq.empty[Path] // 접근 불가 시 무시
            }
        }

        val allSamples: Seq[Key] = allFilePaths.flatMap { filePath =>
            val iterator = new FileRecordIterator(filePath)
            try {
                self.sampleKeys(iterator, samplingRatio)
            } finally {
                iterator.close()
            }
        }

        allSamples
    }
}
    
trait SamplingPolicy {
    val KEY_SIZE: Int
    val MEMORY_SIZE: Long
    val MAX_MEMORY_USAGE_RATIO: Double
    val BYTES_PER_MACHINE: Long
    //val MAX_BYTES_PER_MACHINE_DESIGNED: Long
    val numWorkers: Int

    lazy val usableMemory = MEMORY_SIZE * MAX_MEMORY_USAGE_RATIO
    // sampling시 사용할 record 수
    lazy val numSampleRecords: Double = min(usableMemory/KEY_SIZE, numWorkers*BYTES_PER_MACHINE/KEY_SIZE.toDouble)

    /**
     * 여러 제약 조건(메모리, 머신당 최대 용량 등)을 고려하여 최종적으로 적용해야 할 샘플링 비율(k/n)을 계산
     * 최종 비율은 (전체 메모리 제약, 개별 머신 설계 제약, 최소 할당 바이트) 중 가장 엄격한 값(최솟값)으로 결정
     * @param numTotalRecords 모든 Worker로부터 보고된 총 레코드 수 (모집단 크기).
     * @return 최종 적용할 샘플링 비율 (0.0 < ratio <= 1.0).
     */
    def calculateSamplingRatio(numTotalRecords: Long): Double = {
        if (numTotalRecords == 0) 0.0
        else min(numSampleRecords/numTotalRecords, 1.0)
    }
}
    
/**
 * PivotSelector 트레이트는 샘플 데이터를 분석하여 파티션 경계를 결정하는 알고리즘 책임을 정의
 */
trait PivotSelector {
    // worker의 개수
    val numWorkers: Int

    // 정렬 시 사용할 ordering
    val ordering: Ordering[Key]

    /**
     * 모든 Worker로부터 모인 샘플 Key 리스트를 분산 정렬의 기준에 따라 정렬
     * @param samples 모든 Worker로부터 수집된 Key 리스트
     * @return 정렬된 Key 리스트
     */
    def sortSamples(samples: Seq[Key]): Seq[Key] = {
        samples.sorted(ordering)
    }

    /**
     * 정렬된 샘플 리스트에서 Worker 수에 맞춰 파티션 경계(Pivot) Key를 선택합니다.
     * @param sortedSamples 정렬된 샘플 Key 리스트
     * @param numWorkers 나눌 파티션(Worker)의 개수
     * @return 최종 파티셔닝 경계 Key 벡터
     */
    def selectPivots(sortedSamples: Seq[Key]): Vector[Key] = {
        val numSamples = sortedSamples.size
        if (numSamples < numWorkers) {
            return Vector.empty[Key]
        }

        val step = numSamples / numWorkers
        val remainder = numSamples % numWorkers
        val pivots = (1 until numWorkers).map { i =>
            val pivotIndex = i * step + min(i, remainder)
            sortedSamples(pivotIndex)
        }

        pivots.toVector
    }
}