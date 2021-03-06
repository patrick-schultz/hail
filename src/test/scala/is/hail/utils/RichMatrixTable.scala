package is.hail.utils

import is.hail.annotations.{Annotation, Inserter, Querier, UnsafeRow}
import is.hail.expr.{EvalContext, Parser}
import is.hail.expr.types._
import is.hail.variant.{Locus, MatrixTable, Variant}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import scala.reflect.ClassTag

class RichMatrixTable(vsm: MatrixTable) {
  def expand(): RDD[(Annotation, Annotation, Annotation)] =
    mapWithKeys[(Annotation, Annotation, Annotation)]((v, s, g) => (v, s, g))

  def expandWithAll(): RDD[(Annotation, Annotation, Annotation, Annotation, Annotation)] =
    mapWithAll[(Annotation, Annotation, Annotation, Annotation, Annotation)]((v, va, s, sa, g) => (v, va, s, sa, g))

  def mapWithAll[U](f: (Annotation, Annotation, Annotation, Annotation, Annotation) => U)(implicit uct: ClassTag[U]): RDD[U] = {
    val localSampleIdsBc = vsm.sparkContext.broadcast(vsm.stringSampleIds)
    val localColValuesBc = vsm.colValuesBc

    rdd
      .flatMap { case (v, (va, gs)) =>
        localSampleIdsBc.value.lazyMapWith2[Annotation, Annotation, U](localColValuesBc.value, gs, { case (s, sa, g) => f(v, va, s, sa, g)
        })
      }
  }

  def mapWithKeys[U](f: (Annotation, Annotation, Annotation) => U)(implicit uct: ClassTag[U]): RDD[U] = {
    val localSampleIdsBc = vsm.sparkContext.broadcast(vsm.stringSampleIds)

    rdd
      .flatMap { case (v, (va, gs)) =>
        localSampleIdsBc.value.lazyMapWith[Annotation, U](gs,
          (s, g) => f(v, s, g))
      }
  }

  def annotateSamplesF(signature: Type, path: List[String], annotation: (Annotation) => Annotation): MatrixTable = {
    val (t, i) = vsm.insertSA(signature, path)
    val sampleIds = vsm.stringSampleIds
    vsm.annotateCols(t, i) { case (_, i) => annotation(sampleIds(i)) }
  }

  def annotateRowsExpr(exprs: (String, String)*): MatrixTable =
    vsm.selectRows(s"annotate(va, {${ exprs.map { case (n, e) => s"`$n`: $e" }.mkString(",") }})")

  def annotateEntriesExpr(exprs: (String, String)*): MatrixTable =
    vsm.selectEntries(s"annotate(g, {${ exprs.map { case (n, e) => s"`$n`: $e" }.mkString(",") }})")

  def querySA(code: String): (Type, Querier) = {
    val st = Map(Annotation.COL_HEAD -> (0, vsm.colType))
    val ec = EvalContext(st)
    val a = ec.a

    val (t, f) = Parser.parseExpr(code, ec)

    val f2: Annotation => Any = { annotation =>
      a(0) = annotation
      f()
    }

    (t, f2)
  }

  def queryGA(code: String): (Type, Querier) = {
    val st = Map(Annotation.ENTRY_HEAD -> (0, vsm.entryType))
    val ec = EvalContext(st)
    val a = ec.a

    val (t, f) = Parser.parseExpr(code, ec)

    val f2: Annotation => Any = { annotation =>
      a(0) = annotation
      f()
    }

    (t, f2)
  }

  def stringSampleIdsAndAnnotations: IndexedSeq[(Annotation, Annotation)] = vsm.stringSampleIds.zip(vsm.colValues)

  def rdd: RDD[(Annotation, (Annotation, Iterable[Annotation]))] = {
    val fullRowType = vsm.rvRowType
    val localEntriesIndex = vsm.entriesIndex
    val rowKeyF = vsm.rowKeysF
    vsm.rvd.rdd.map { rv =>
      val rvc = rv.copy()
      val fullRow = new UnsafeRow(fullRowType, rvc)
      val row = fullRow.deleteField(localEntriesIndex)
      (rowKeyF(fullRow), (row, fullRow.getAs[IndexedSeq[Any]](localEntriesIndex)))
    }
  }

  def variantRDD: RDD[(Variant, (Annotation, Iterable[Annotation]))] =
    rdd.map { case (v, (va, gs)) =>
      Variant.fromLocusAlleles(v) -> (va, gs)
    }

  def typedRDD[RK](implicit rkct: ClassTag[RK]): RDD[(RK, (Annotation, Iterable[Annotation]))] =
    rdd.map { case (v, (va, gs)) =>
      (v.asInstanceOf[RK], (va, gs))
    }

  def variants: RDD[Variant] = variantRDD.keys

  def locusAlleles: RDD[(Locus, IndexedSeq[String])] =
    variants.map { v =>
      (v.locus, v.alleles)
    }

  def variantsAndAnnotations: RDD[(Variant, Annotation)] =
    variantRDD.map { case (v, (va, gs)) => (v, va) }

  def reorderCols(newIds: Array[Annotation]): MatrixTable = {
    require(newIds.length == vsm.numCols)
    require(newIds.areDistinct())

    val sampleSet = vsm.colKeys.toSet[Annotation]
    val newSampleSet = newIds.toSet

    val notInDataset = newSampleSet -- sampleSet
    if (notInDataset.nonEmpty)
      fatal(s"Found ${ notInDataset.size } ${ plural(notInDataset.size, "sample ID") } in new ordering that are not in dataset:\n  " +
        s"@1", notInDataset.truncatable("\n  "))

    val oldIndex = vsm.colKeys.zipWithIndex.toMap
    val newToOld = newIds.map(oldIndex)

    vsm.chooseCols(newToOld)
  }
}
