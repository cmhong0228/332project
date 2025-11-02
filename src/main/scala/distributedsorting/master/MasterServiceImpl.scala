package distributedsorting.master

import distributedsorting._import scala.concurrent.{Future, Promise}
import scala.jdk.CollectionConverters._
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

class MasterServiceImpl(numWorkers: Int) extends MasterServiceGrpc.MasterService {

    // 동시성 처리를 위한 맵과 아토믹 변수
    private val recordCounts = new ConcurrentHashMap[String, java.lang.Long]()
    private val totalRecords = new AtomicLong(0)
    
    // 샘플 Key를 안전하게 합병하기 위한 동기화된 리스트
    private val allSampleKeys = scala.collection.mutable.ArrayBuffer.empty[Key]

    private val KEY_LENGTH = ???
    private val RECORD_LENGTH = ???
    private val keyOrdering = ???

    override def reportRecordCount(request: RecordCountReport): Future[SamplingRatio] = ???

    override def sendSampleKeys(request: SampleKeyList): Future[SampleDecision] = ???

    private def selectPivots(sortedSamples: Seq[Key], numWorkers: Int): Seq[Key] = ???
}