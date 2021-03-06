package is.hail.sparkextras

import is.hail.annotations._
import is.hail.rvd.{OrderedRVD, OrderedRVDPartitioner, OrderedRVDType}
import is.hail.utils._
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

// FIXME(DK): Gotta update this for contextrdd

/**
  * Repartition prev to comply with newPartitioner, using narrow dependencies.
  * Assumes new key type is a prefix of old key type, so no reordering is
  * needed. No assumption should need to be made about partition keys, but currently
  * assumes old partition key type is a prefix of the new partition key type.
  */
object RepartitionedOrderedRDD {
  def apply(prev: OrderedRVD, newPartitioner: OrderedRVDPartitioner): RepartitionedOrderedRDD = {
    new RepartitionedOrderedRDD(
      prev.rdd,
      prev.typ,
      prev.partitioner.broadcast(prev.rdd.sparkContext),
      newPartitioner.broadcast(prev.rdd.sparkContext))
  }
}

class RepartitionedOrderedRDD(
    prevRDD: RDD[RegionValue],
    typ: OrderedRVDType,
    oldPartitionerBc: Broadcast[OrderedRVDPartitioner],
    newPartitionerBc: Broadcast[OrderedRVDPartitioner])
  extends RDD[RegionValue](prevRDD.sparkContext, Nil) { // Nil since we implement getDependencies

  private def oldPartitioner = oldPartitionerBc.value
  private def newPartitioner = newPartitionerBc.value

//  require(newPartitioner.kType isPrefixOf prev.typ.kType)
  // There should really be no precondition on partition keys. Drop this when
  // we're able
  require(typ.pkType isPrefixOf newPartitioner.pkType)

  def getPartitions: Array[Partition] = {
    Array.tabulate[Partition](newPartitioner.numPartitions) { i =>
      RepartitionedOrderedRDD2Partition(
        i,
        dependency.getParents(i).toArray.map(prevRDD.partitions),
        newPartitioner.rangeBounds(i).asInstanceOf[Interval])
    }
  }

  override def compute(partition: Partition, context: TaskContext): Iterator[RegionValue] = {
    val ordPartition = partition.asInstanceOf[RepartitionedOrderedRDD2Partition]
    val it = ordPartition.parents.iterator
      .flatMap { parentPartition =>
        prevRDD.iterator(parentPartition, context)
      }
    OrderedRVIterator(typ, it).restrictToPKInterval(ordPartition.range)
  }

  val dependency = new OrderedDependency(oldPartitionerBc, newPartitionerBc, prevRDD)

  override def getDependencies: Seq[Dependency[_]] = Seq(dependency)
}

class OrderedDependency(oldPartitionerBc: Broadcast[OrderedRVDPartitioner], newPartitionerBc: Broadcast[OrderedRVDPartitioner], rdd: RDD[RegionValue])
  extends NarrowDependency[RegionValue](rdd) {

  private def oldPartitioner = oldPartitionerBc.value
  private def newPartitioner = newPartitionerBc.value

  // no precondition on partition keys
//  require(newPartitioner.kType isPrefixOf prev.typ.kType)
  // There should really be no precondition on partition keys. Drop this when
  // we're able
  require(oldPartitioner.pkType isPrefixOf newPartitioner.pkType)

  override def getParents(partitionId: Int): Seq[Int] = {
    val partBounds =
      newPartitioner.rangeBounds(partitionId).asInstanceOf[Interval]
    oldPartitioner.getPartitionRange(partBounds)
  }
}

case class RepartitionedOrderedRDD2Partition(
    index: Int,
    parents: Array[Partition],
    range: Interval)
  extends Partition
