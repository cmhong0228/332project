package distributedsorting.worker

import distributedsorting.distributedsorting._
import scala.collection.concurrent.TrieMap

/**
 * 테스트용 임시 데이터 구조 및 헬퍼
 * 
 * 주의: 이 파일의 모든 코드는 임시방편입니다!
 * 실제 shuffle 구현 시 다른 팀과 합의하여 프로덕션 코드로 이동 필요
 */
object TestHelpers {
    
    // =========================================================================
    // Shuffle Phase 테스트용 임시 타입들
    // =========================================================================
    
    /**
     * Shuffle Phase 실행 결과
     * 
     * TODO: 실제 구현 시 더 상세한 정보 필요 (실패 원인, 재시도 정보 등)
     */
    case class ShuffleResult(
        successCount: Int,
        failureCount: Int
    )
    
    /**
     * 파티션별 파일 구조 표현 (Map O/D)
     * 
     * TODO: 실제로는 sort/partition phase의 결과 구조와 통합 필요
     * TODO: 파일 크기, 레코드 수 등 메타데이터 추가 필요
     */
    case class FileStructure(
        partitionToFiles: Map[Int, Seq[FileId]]
    ) {
        /**
         * 특정 파티션에 필요한 파일들
         */
        def getFilesForPartition(partitionId: Int): Set[FileId] = {
            partitionToFiles.getOrElse(partitionId, Seq.empty).toSet
        }
        
        /**
         * 모든 파일
         */
        def allFiles: Set[FileId] = {
            partitionToFiles.values.flatten.toSet
        }
    }
    
    /**
     * 로컬 환경 테스트용 워커 레지스트리
     * 
     * 역할: 같은 프로세스 내 다른 워커의 LocalFileTransport 인스턴스를 찾아줌
     * 
     * 실제 환경에서는:
     * - Master에게 worker registration 수행
     * - 다른 워커의 네트워크 주소(IP:Port)를 받음
     * - gRPC 클라이언트로 직접 통신
     * 
     * TODO: 실제 구현 시 제거하고 Master registration으로 대체
     */
    class WorkerRegistry extends LocalTransportRegistry {
        private val workers = TrieMap[Int, LocalFileTransport]()
        
        /**
         * 워커 등록
         */
        override def register(workerId: Int, transport: LocalFileTransport): Unit = {
            workers.put(workerId, transport)
            println(s"[TestRegistry] Worker $workerId registered")
        }
        
        /**
         * 워커 조회
         */
        override def get(workerId: Int): Option[LocalFileTransport] = {
            workers.get(workerId)
        }
        
        /**
         * 모든 워커 ID 조회
         */
        def getAllWorkerIds: Set[Int] = {
            workers.keySet.toSet
        }
        
        /**
         * 레지스트리 종료
         */
        def shutdown(): Unit = {
            workers.clear()
            println(s"[TestRegistry] Registry shutdown")
        }
    }
    
    object WorkerRegistry {
        def apply(): WorkerRegistry = new WorkerRegistry()
    }
    
    // =========================================================================
    // 테스트용 헬퍼 메서드들
    // =========================================================================
    
    /**
     * 테스트용 FileStructure 생성
     * 
     * 각 워커가 모든 파티션에 대해 하나의 파일을 생성했다고 가정
     * 예: Worker 0 → file_0_0_0.dat, file_0_1_0.dat, file_0_2_0.dat, ...
     */
    def createTestFileStructure(numWorkers: Int): FileStructure = {
        val partitionToFiles = (0 until numWorkers).map { partitionId =>
            // 파티션 X는 모든 워커의 file_*_X_0.dat 필요
            val files = (0 until numWorkers).map { sourceWorkerId =>
                FileId(sourceWorkerId, partitionId, 0)
            }
            partitionId -> files
        }.toMap
        
        FileStructure(partitionToFiles)
    }
}
