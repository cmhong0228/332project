package distributedsorting

/**
 * distributedsorting 패키지 전체에서 사용할 멤버 정의
 */
package object distributedsorting {
    case class FileId(
        sourceWorkerId: Int, // sourceworkerId after sort/partition
        partitionId: Int,    // partitionId
        sortedRunId: Int     // index (for same i, j)
    ) {
        def toFileName: String = s"file_${sourceWorkerId}_${partitionId}_${sortedRunId}.dat"
    }

    /*
     * record를 저장할 type
     */
    type Record = Array[Byte]

    /*
     * key를 저장할 type
     * Record와 같은 타입이지만 편의상 분리, key만을 저장하는 경우 사용
     */
    type Key = Array[Byte]

    /*
     * RecordOrdering을 생성하는 factory method
     * @param keyLength key의 길이
     * @param recordLength record의 길이 (key+value)
     * @return Record를 비교하는 Ordering instance
     */
     def createRecordOrdering(keyLength: Int, recordLength: Int): Ordering[Record] = {
        (recA: Record, recB: Record) => {
            assert(recA.length == recordLength, s"Invalid Record A length: ${recA.length}, expected $recordLength")
            assert(recB.length == recordLength, s"Invalid Record B length: ${recB.length}, expected $recordLength")

            var i = 0
            var result = 0

            while (i < keyLength && result == 0) {
                val a = java.lang.Byte.toUnsignedInt(recA(i))
                val b = java.lang.Byte.toUnsignedInt(recB(i))
                result = a.compareTo(b) // a < b: -, a > b: +, a==b: 0
                i += 1
            }

            result
        }
     }
}
