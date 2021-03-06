package is.hail.rvd

import is.hail.annotations.{JoinedRegionValue, OrderedRVIterator, RegionValue}
import org.apache.spark.rdd.RDD
import is.hail.utils.fatal

class KeyedOrderedRVD(val rvd: OrderedRVD, val key: Array[String]) {
  val typ: OrderedRVDType = rvd.typ
  val (kType, _) = rvd.rowType.select(key)
  require(kType isPrefixOf rvd.typ.kType)

  private def checkJoinCompatability(right: KeyedOrderedRVD) {
    if (!(kType isIsomorphicTo kType))
      fatal(
        s"""Incompatible join keys.  Keys must have same length and types, in order:
           | Left key type: ${ kType.toString }
           | Right key type: ${ kType.toString }
         """.stripMargin)
  }

  def orderedJoin(
    right: KeyedOrderedRVD,
    joinType: String,
    joiner: Iterator[JoinedRegionValue] => Iterator[RegionValue],
    joinedType: OrderedRVDType
  ): OrderedRVD = {
    checkJoinCompatability(right)
    val lTyp = typ
    val rTyp = right.typ

    val newPartitioner =
      this.rvd.partitioner.enlargeToRange(right.rvd.partitioner.range)
    val repartitionedLeft =
      this.rvd.constrainToOrderedPartitioner(this.typ, newPartitioner)
    val repartitionedRight =
      right.rvd.constrainToOrderedPartitioner(right.typ, newPartitioner)
    val compute: (OrderedRVIterator, OrderedRVIterator) => Iterator[JoinedRegionValue] =
      (joinType: @unchecked) match {
        case "inner" => _.innerJoin(_)
        case "left" => _.leftJoin(_)
        case "right" => _.rightJoin(_)
        case "outer" => _.outerJoin(_)
      }
    val joinedRDD =
      repartitionedLeft.rdd.zipPartitions(repartitionedRight.rdd, true) {
        (leftIt, rightIt) =>
          joiner(compute(
            OrderedRVIterator(lTyp, leftIt),
            OrderedRVIterator(rTyp, rightIt)))
    }

    new OrderedRVD(joinedType, newPartitioner, joinedRDD)
  }

  def orderedJoinDistinct(
    right: KeyedOrderedRVD,
    joinType: String,
    joiner: Iterator[JoinedRegionValue] => Iterator[RegionValue],
    joinedType: OrderedRVDType
  ): OrderedRVD = {
    checkJoinCompatability(right)
    val rekeyedLTyp = new OrderedRVDType(typ.partitionKey, key, typ.rowType)
    val rekeyedRTyp = new OrderedRVDType(right.typ.partitionKey, right.key, right.typ.rowType)

    val newPartitioner = this.rvd.partitioner
    val repartitionedRight =
      right.rvd.constrainToOrderedPartitioner(right.typ, newPartitioner)
    val compute: (OrderedRVIterator, OrderedRVIterator) => Iterator[JoinedRegionValue] =
      (joinType: @unchecked) match {
        case "inner" => _.innerJoinDistinct(_)
        case "left" => _.leftJoinDistinct(_)
      }
    val joinedRDD =
      this.rvd.rdd.zipPartitions(repartitionedRight.rdd, true) {
        (leftIt, rightIt) =>
          joiner(compute(
            OrderedRVIterator(rekeyedLTyp, leftIt),
            OrderedRVIterator(rekeyedRTyp, rightIt)))
    }

    new OrderedRVD(joinedType, newPartitioner, joinedRDD)
  }

  def orderedZipJoin(right: KeyedOrderedRVD): RDD[JoinedRegionValue] = {
    val newPartitioner = rvd.partitioner.enlargeToRange(right.rvd.partitioner.range)

    val repartitionedLeft = rvd.constrainToOrderedPartitioner(typ, newPartitioner)
    val repartitionedRight = right.rvd.constrainToOrderedPartitioner(right.typ, newPartitioner)

    val leftType = this.typ
    val rightType = right.typ
    repartitionedLeft.rdd.zipPartitions(repartitionedRight.rdd, true){ (leftIt, rightIt) =>
      OrderedRVIterator(leftType, leftIt).zipJoin(OrderedRVIterator(rightType, rightIt))
    }
  }
}
