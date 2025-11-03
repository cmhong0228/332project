package distributedsorting.worker

import distributedsorting.distributedsorting._
import distributedsorting.logic._
import java.nio.file.Path
import io.grpc.ManagedChannel
import distributedsorting.distributedsorting.MasterServiceGrpc.MasterServiceStub

trait MasterClient { self: RecordCountCalculator with RecordExtractor => 
    // Master와의 통신 스텁 및 Worker ID 정의
    val masterClient: MasterServiceStub
    val workerId: String
            
    // ======================= Sampling =======================
    /*
     * Sampling(Worker)
     * 역할
     * * master에게 자신이 갖고있는 data 전달 후 sample ratio 받음
     * * master에게 sample data 보낸 후 pivots 받음
     * pivots: Vector[key]
     */

    /**
     * Worker 노드의 샘플링 프로세스를 실행하는 함수
     * Master와의 2단계 통신(보고 -> 비율 획득, 샘플 전송 -> 피벗 획득)을 조정하고, 최종 피벗을 반환
     * @param inputDirs Worker가 처리할 데이터가 위치한 로컬 디스크 경로 목록
     * @return Master로부터 받은, 데이터를 파티셔닝할 최종 경계 Key 벡터 (Pivots)
     */
    def executeSampling(inputDirs: Seq[Path]): Vector[Key] = ???
}