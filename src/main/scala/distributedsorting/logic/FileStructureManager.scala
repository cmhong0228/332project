package distributedsorting.logic

import distributedsorting.distributedsorting._
import java.nio.file.{Files, Path}
import scala.collection.JavaConverters._

object FileStructureManager {

    /**
     * Worker에서 Sort/partition 끝난 후 자신이 만든 File_id들을 취합
     * 
     * @param partitionOutputDir file_i_j_k.dat 형태로 파일들이 저장된 디렉토리
     * @return 해당 디렉토리의 파일들에 대한 Set[FileId]
     */
    def collectLocalFileIds(partitionOutputDir: Path): Set[FileId] = {
        Files.list(partitionOutputDir)
            .iterator()
            .asScala
            .filter(Files.isRegularFile(_))
            .flatMap { path =>
                val fileName = path.getFileName.toString
                // file_i_j_k.dat 형식 파싱
                val pattern = """file_(\d+)_(\d+)_(\d+)\.dat""".r
                fileName match {
                    case pattern(i, j, k) => 
                        Some(FileId(i.toInt, j.toInt, k.toInt))
                    case _ => None
                }
            }
            .toSet
    }

    /**
     * Master에서 worker들로부터 받은 Set[FileId]들을 취합
     * 
     * @param workerFileSets 각 워커의 FileId 집합들
     * @return 파티션별 파일 맵 Map[partitionId => Seq[FileId]]
     */
    def aggregateToPartitionMap(
        workerFileSets: Map[Int, Set[FileId]]
    ): Map[Int, Seq[FileId]] = {
        workerFileSets.values
            .flatten
            .groupBy(_.partitionId)
            .view
            .mapValues(_.toSeq)
            .toMap
    }

    /**
     * FileStructure(Map)에서 neededFile 계산
     * 
     * @param partitionMap Map[partitionId => Seq[FileId]]
     * @param myPartitionId 내가 담당할 파티션 ID
     * @return 내가 가져와야 할 파일들 Set[FileId]
     */
    def getNeededFiles(
        partitionMap: Map[Int, Seq[FileId]], 
        myPartitionId: Int
    ): Set[FileId] = {
        partitionMap.getOrElse(myPartitionId, Seq.empty).toSet
    }
}