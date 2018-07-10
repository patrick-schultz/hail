package is.hail.rvd

import is.hail.annotations.UnsafeIndexedSeq
import is.hail.expr.types._
import is.hail.utils.Interval
import org.apache.spark.sql.Row
import org.scalatest.testng.TestNGSuite
import org.testng.annotations.Test

class OrderedRVDPartitionerSuite extends TestNGSuite {
  val partitioner =
    new OrderedRVDPartitioner(
      Array("A", "B"),
      TStruct(("A", TInt32()), ("B", TInt32()), ("C", TInt32())),
      Array(
        Interval(Row(1, 0), Row(4, 3), true, false),
        Interval(Row(4, 3), Row(7, 9), true, false),
        Interval(Row(7, 11), Row(10, 0), true, true))
    )

  @Test def testGetPartitionWithPartitionKeys() {
    assert(partitioner.getSafePartitionUpperBound(Row(-1, 7)) == -1)
    assert(partitioner.getSafePartitionLowerBound(Row(-1, 7)) == 0)
    assert(partitioner.getSafePartitionUpperBound(Row(4, 2)) == 0)
    assert(partitioner.getSafePartitionLowerBound(Row(4, 2)) == 0)
    assert(partitioner.getSafePartitionLowerBound(Row(4, 3)) == 1)
    assert(partitioner.getSafePartitionUpperBound(Row(4, 3)) == 1)
    assert(partitioner.getSafePartitionLowerBound(Row(5, -10259)) == 1)
    assert(partitioner.getSafePartitionUpperBound(Row(7, 9)) == 1)
    assert(partitioner.getSafePartitionLowerBound(Row(7, 9)) == 2)
    assert(partitioner.getSafePartitionUpperBound(Row(12, 19)) == 2)
    assert(partitioner.getSafePartitionLowerBound(Row(12, 19)) == 3)
  }

  @Test def testGetPartitionWithLargerKeys() {
    assert(partitioner.getSafePartitionUpperBound(Row(0, 1, 3)) == -1)
    assert(partitioner.getSafePartitionLowerBound(Row(0, 1, 3)) == 0)
    assert(partitioner.getSafePartitionUpperBound(Row(2, 7, 5)) == 0)
    assert(partitioner.getSafePartitionLowerBound(Row(2, 7, 5)) == 0)
    assert(partitioner.getSafePartitionUpperBound(Row(4, 2, 1, 2.7, "bar")) == 0)
    assert(partitioner.getSafePartitionUpperBound(Row(7, 9, 7)) == 1)
    assert(partitioner.getSafePartitionLowerBound(Row(7, 9, 7)) == 2)
    assert(partitioner.getSafePartitionLowerBound(Row(11, 1, 42)) == 3)
  }

   @Test def testGetPartitionPKWithSmallerKeys() {
     assert(partitioner.getSafePartitionUpperBound(Row(2)) == 0)
     assert(partitioner.getSafePartitionLowerBound(Row(2)) == 0)
     assert(partitioner.getSafePartitionUpperBound(Row(4)) == 1)
     assert(partitioner.getSafePartitionLowerBound(Row(4)) == 0)
     assert(partitioner.getSafePartitionUpperBound(Row(11)) == 2)
     assert(partitioner.getSafePartitionLowerBound(Row(11)) == 3)
   }

  @Test def testGetPartitionRange() {
    assert(partitioner.getPartitionRange(Interval(Row(3, 4), Row(7, 11), true, true)) == Seq(0, 1, 2))
    assert(partitioner.getPartitionRange(Interval(Row(3, 4), Row(7, 9), true, false)) == Seq(0, 1))
    assert(partitioner.getPartitionRange(Interval(Row(-1, 7), Row(0, 9), true, false)) == Seq())
  }

  @Test def testGetSafePartitionKeyRange() {
    assert(partitioner.getSafePartitionKeyRange(Row(0, 0)).isEmpty)
    assert(partitioner.getSafePartitionKeyRange(Row(7, 10)).isEmpty)
    assert(partitioner.getSafePartitionKeyRange(Row(7, 11)) == Range.inclusive(2, 2))
  }

  @Test def testFixup() {
    val partitioner = OrderedRVDPartitioner.fixup(
      Array("A", "B"),
      TStruct(("A", TInt32()), ("B", TInt32()), ("C", TInt32())),
      Array(
        Interval(Row(1, 0, 4), Row(4, 3, 2), true, false),
        Interval(Row(4, 3, 5), Row(7, 9, 1), true, false),
        Interval(Row(7, 11, 3), Row(10, 0, 1), true, true),
        Interval(Row(10, 0, 2), Row(10, 0, 15), false, false),
        Interval(Row(10, 0, 20), Row(10, 1, 7), true, false))
    )
    assert(partitioner.rangeBounds sameElements
      Array(
        Interval(Row(1, 0, 4), Row(4, 3), true, true),
        Interval(Row(4, 3), Row(7, 9, 1), false, false),
        Interval(Row(7, 11, 3), Row(10, 0), true, true),
        Interval(Row(10, 0), Row(10, 1, 7), false, false))
    )
  }
}
