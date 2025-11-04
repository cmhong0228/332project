package distributedsorting

import munit.FunSuite 
import distributedsorting.createRecordOrdering
import distributedsorting.Record

class OrderingSuite extends FunSuite {

    trait OrderingTestSetupDiffLength {
        val KEY_LENGTH = 4
        val RECORD_LENGTH = 8

        val recordOrdering: Ordering[Record] = createRecordOrdering(KEY_LENGTH, RECORD_LENGTH)
        def compare(recA: Record, recB: Record): Int = recordOrdering.compare(recA, recB)

        val record1 = Array[Byte](1, 2, 3, 4, 5, 6, 7, 8)
        val record2 = Array[Byte](1, 2, 3, 4, 5, 6, 7, 8)
    }

    trait OrderingTestSetupSameLength {
        val KEY_LENGTH = 4
        val RECORD_LENGTH = 4

        val recordOrdering: Ordering[Record] = createRecordOrdering(KEY_LENGTH, RECORD_LENGTH)
        def compare(recA: Record, recB: Record): Int = recordOrdering.compare(recA, recB)

        val record1 = Array[Byte](1, 2, 3, 4)
        val record2 = Array[Byte](1, 2, 3, 4)
    }

    test("Same record should return 0") {
        new OrderingTestSetupDiffLength {
            assertEquals(compare(record1, record2), 0)
        }
        
        new OrderingTestSetupSameLength {
            assertEquals(compare(record1, record2), 0)
        }
    }

    test("Same key and different value should return 0") {
        new OrderingTestSetupDiffLength {
            val recB = Array[Byte](1, 2, 3, 4, 10, 20, 30, 40)

            assertEquals(compare(record1, recB), 0)
        }
    }

    test("If first record is smaller than second, then should return negative") {
        new OrderingTestSetupDiffLength {
            val recB = Array[Byte](1, 2, 4, 4, 1, 2, 3, 4)

            assert(compare(record1, recB) < 0)
        }
    }
    
    test("If first record is larger than second, then should return positive") {
        new OrderingTestSetupDiffLength {
            val recB = Array[Byte](1, 1, 3, 4, 1, 2, 3, 4)

            assert(compare(record1, recB) > 0)
        }
    }
    
    test("Compare should be conducted in unsigned manner") {
        new OrderingTestSetupDiffLength {
            val unsigned1: Byte = 1
            val unsigned128: Byte = -128

            val recA = Array[Byte](unsigned1, 0, 0, 0, 0, 0, 0, 0)
            val recB = Array[Byte](unsigned128, 0, 0, 0, 0, 0, 0, 0)

            assert(compare(recA, recB) < 0)
        }
    }

    test("Assertion Error test") {
        new OrderingTestSetupDiffLength {
            override val RECORD_LENGTH = 3
            intercept[AssertionError] {
                compare(record1, record2)
            }
        }
        
        new OrderingTestSetupDiffLength {
            val recA = Array[Byte](0, 0, 0, 0, 0, 0, 0)
            intercept[AssertionError] {
                compare(recA, record2)
            }
        }

        new OrderingTestSetupDiffLength {
            val recB = Array[Byte](0, 0, 0, 0, 0, 0, 0)
            intercept[AssertionError] {
                compare(record1, recB)
            }
        }
    }
}