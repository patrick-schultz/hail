package is.hail.annotations

import is.hail.annotations.aggregators._
import org.testng.annotations.Test
import org.scalatest.testng.TestNGSuite

class ApproxCDFAggregatorSuite extends TestNGSuite {
  @Test
  def testMerge() {
    val helper: ApproxCDFHelper[Int] = ApproxCDFHelper.IntHelper
    val array: Array[Int] = Array(1,3,5,0,0,0,2,4,6)
    helper.merge(array, 0, 3, array, 6, 9, array, 3)
    assert(array.view(3, 9) sameElements Range(1, 7))
  }

  @Test
  def testCompactLevel() {
    val rand = new java.util.Random(1) // first Boolean is `true`
    val levels: Array[Int] = Array(0,3,7,10)
    val items: Array[Int] = Array(7,2,4, 1,3,6,8, 0,5,9)
    val combiner = new ApproxCDFCombiner(levels, items, 3, rand)
    combiner.compactLevel(0, shiftLowerLevels = true, keep = 0)
    assert(items.view(1,10) sameElements Array(2, 1,3,6,7,8, 0,5,9))
  }

  @Test
  def testCompactLevelAndShift() {
    val rand = new java.util.Random(1) // first Boolean is `true`
    val levels: Array[Int] = Array(0,3,6,9)
    val items: Array[Int] = Array(7,2,4, 1,3,8, 0,5,9)
    val combiner = new ApproxCDFCombiner(levels, items, 3, rand)
    combiner.compactLevel(1, shiftLowerLevels = true)
    assert(items.view(1,9) sameElements Array(7,2,4, 1, 0,5,8,9))
  }

  @Test
  def testCompactLevelKeep() {
    val rand = new java.util.Random(1) // first Boolean is `true`
    val levels: Array[Int] = Array(0,3,7,10)
    val items: Array[Int] = Array(7,2,4, 1,3,6,8, 0,5,9)
    val combiner = new ApproxCDFCombiner(levels, items, 3, rand)
    combiner.compactLevel(1, shiftLowerLevels = true, keep = 1)
    assert(items.view(1,10) sameElements Array(7,2,4, 1,8, 0,5,6,9))
  }
}
