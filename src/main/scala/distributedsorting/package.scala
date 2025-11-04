package distributedsorting

/**
 * distributedsorting 패키지 전체에서 사용할 멤버 정의
 */
package object distributedsorting {
    /*
     * record를 저장할 type
     */
    type Record = Array[Byte]

    /*
     * key를 저장할 type
     * record에서 따로 key를 분리해서 고려하지는 않음
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
            assert(keyLength <= recordLength, "Invalid keyLength and recordLength")
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