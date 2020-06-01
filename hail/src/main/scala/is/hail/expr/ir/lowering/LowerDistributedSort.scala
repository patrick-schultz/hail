package is.hail.expr.ir.lowering

import is.hail.annotations.{Annotation, ExtendedOrdering, Region, SafeRow, UnsafeRow}
import is.hail.asm4s.{AsmFunction1RegionLong, LongInfo, classInfo}
import is.hail.expr.ir._
import is.hail.types.physical.{PArray, PStruct, PTuple}
import is.hail.types.virtual.TStruct
import is.hail.rvd.RVDPartitioner
import is.hail.utils._
import org.apache.spark.sql.Row

object LowerDistributedSort {
  def localSort(ctx: ExecuteContext, stage: TableStage, sortFields: IndexedSeq[SortField]): TableStage = {
    val numPartitions = stage.partitioner.numPartitions
    val collected = stage.collectWithGlobals()

    val (resultPType: PStruct, f) = ctx.timer.time("LowerDistributedSort.localSort.compile")(Compile[AsmFunction1RegionLong](ctx,
      FastIndexedSeq(),
      FastIndexedSeq(classInfo[Region]), LongInfo,
      collected,
      print = None,
      optimize = true))

    val fRunnable = ctx.timer.time("LowerDistributedSort.localSort.initialize")(f(0, ctx.r))
    val resultAddress = ctx.timer.time("LowerDistributedSort.localSort.run")(fRunnable(ctx.r))
    val rowsAndGlobal = ctx.timer.time("LowerDistributedSort.localSort.toJavaObject")(SafeRow.read(resultPType, resultAddress)).asInstanceOf[Row]

    val rowsType = resultPType.fieldType("rows").asInstanceOf[PArray]
    val rowType = rowsType.elementType.asInstanceOf[PStruct]

    val sortColIndexOrd = sortFields.map { case SortField(n, so) =>
      val i = rowType.fieldIdx(n)
      val f = rowType.fields(i)
      val fo = f.typ.virtualType.ordering
      if (so == Ascending) fo else fo.reverse
    }.toArray

    val getSortKey: Any => Any = {
      val idxs: Array[Int] = sortFields.map { case SortField(n, _) =>
        rowType.fieldIdx(n)
      }.toArray
      row => Row(idxs.map(row.asInstanceOf[Row].apply): _*)
    }

    val ord: Ordering[Annotation] = ExtendedOrdering.rowOrdering(sortColIndexOrd).toOrdering
    val rows = rowsAndGlobal.getAs[IndexedSeq[Annotation]](0)
    val sortedRows = ctx.timer.time("LowerDistributedSort.localSort.sort")(rows.sortBy(getSortKey)(ord))
    val nPartitionsAdj = math.max(math.min(sortedRows.length, numPartitions), 1)
    val itemsPerPartition = (sortedRows.length.toDouble / nPartitionsAdj).ceil.toInt

    val kType = TStruct(sortFields.takeWhile(_.sortOrder == Ascending).map(f => (f.field, rowType.virtualType.fieldType(f.field))): _*)
    val kIndex = kType.fieldNames.map(f => rowType.fieldIdx(f))
    val partitioner = new RVDPartitioner(kType,
      sortedRows.grouped(itemsPerPartition).map { group =>
        val first = group.head.asInstanceOf[Row].select(kIndex)
        val last = group.last.asInstanceOf[Row].select(kIndex)
        Interval(first, last, includesStart = true, includesEnd = true)
      }.toArray,
      allowedOverlap = kType.size)

    TableStage(
      letBindings = FastIndexedSeq.empty, Set(),
      globals = Literal(resultPType.fieldType("global").virtualType, rowsAndGlobal.get(1)),
      partitioner = partitioner,
      contexts = mapIR(
        StreamGrouped(
          ToStream(Literal(rowsType.virtualType, sortedRows)),
          I32(itemsPerPartition))
        )(ToArray(_)),
      ctxRef => ToStream(ctxRef))
  }
}
