package distributedsorting.worker

import distributedsorting._
import distributedsorting.logic.Sampler
import java.nio.file.Path

import io.grpc.ManagedChannel
import distributedsorting.MasterServiceGrpc 

type MasterClientStub = MasterServiceGrpc.MasterServiceFutureStub

trait MasterClient { self: Sampler => 

    // Master와의 통신 스텁 및 Worker ID 정의
    val masterClient: MasterClientStub
    val workerId: String
        
    def runSamplingPhase(inputDirs: Seq[Path]): PartitionInfo = ???
}