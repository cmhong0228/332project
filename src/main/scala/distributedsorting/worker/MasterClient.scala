package distributedsorting.worker

import distributedsorting.distributedsorting._
import distributedsorting.logic.Sampler
import java.nio.file.Path
import io.grpc.ManagedChannel
import distributedsorting.distributedsorting.MasterServiceGrpc.MasterServiceStub

trait MasterClient { self: Sampler => 
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

    def runSamplingPhase(inputDirs: Seq[Path]): Vector[Key] = ???
}