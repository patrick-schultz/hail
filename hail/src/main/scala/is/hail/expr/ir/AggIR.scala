package is.hail.expr.ir

import is.hail.expr.types.BaseType
import is.hail.expr.types.virtual._
import is.hail.utils._

object PrimAgg {
  def fromOldAggOp(aggSig: AggSignature): PrimAgg = aggSig.op match {
    case ApproxCDF() => ApproxCDFAgg
    case CallStats() => CallStatsAgg
    case Collect() => CollectAgg(aggSig.seqOpArgs.head)
    case CollectAsSet() => CollectAsSetAgg(aggSig.seqOpArgs.head)
    case Count() => CountAgg
    case Downsample() => DownsampleAgg(coerce[TArray](aggSig.seqOpArgs(2)).elementType)
    case LinearRegression() => LinearRegressionAgg
    case Max() => MaxAgg(aggSig.seqOpArgs.head)
    case Min() => MinAgg(aggSig.seqOpArgs.head)
    case Product() => ProductAgg(aggSig.seqOpArgs.head)
    case Sum() => SumAgg(aggSig.seqOpArgs.head)
    case Take() => TakeAgg(aggSig.seqOpArgs.head)
    case TakeBy() => TakeByAgg(aggSig.seqOpArgs(0), aggSig.seqOpArgs(1))
    case PrevNonnull() => PrevNonnullAgg(aggSig.seqOpArgs.head)
  }
}
sealed trait PrimAgg {
  def seqArgs: IndexedSeq[Type]
  def resultType: Type
  def isCommutative: Boolean = this match {
    case _: TakeAgg | _: CollectAgg | _: PrevNonnullAgg | _: TakeByAgg => false
    case _ => true
  }
}
case object ApproxCDFAgg extends PrimAgg {
  val seqArgs = FastIndexedSeq(TFloat64())
  val resultType = is.hail.annotations.aggregators.QuantilesAggregator.resultType.virtualType
}
case object CallStatsAgg extends PrimAgg {
  val seqArgs = FastIndexedSeq(TCall())
  val resultType = agg.CallStatsState.resultType.virtualType
}
final case class CollectAgg(typ: Type) extends PrimAgg {
  val seqArgs = FastIndexedSeq(typ)
  val resultType = TArray(typ)
}
final case class CollectAsSetAgg(typ: Type) extends PrimAgg {
  val seqArgs = FastIndexedSeq(typ)
  val resultType = TSet(typ)
}
case object CountAgg extends PrimAgg {
  val seqArgs = FastIndexedSeq()
  val resultType = TInt64()
}
final case class DownsampleAgg(labelType: Type) extends PrimAgg {
  val seqArgs = FastIndexedSeq(TFloat64(), TFloat64(), TArray(labelType))
  val resultType = TArray(TTuple(TFloat64(), TFloat64(), TArray(labelType)))
}
case object LinearRegressionAgg extends PrimAgg {
  val seqArgs = FastIndexedSeq(TFloat64(), TInt64())
  val resultType = agg.LinearRegressionAggregator.resultType.virtualType
}
final case class MaxAgg(typ: Type) extends PrimAgg {
  assert(typ.isInstanceOf[TBoolean] || typ.isInstanceOf[TNumeric])
  val seqArgs = FastIndexedSeq(typ)
  def resultType = typ
}
final case class MinAgg(typ: Type) extends PrimAgg {
  assert(typ.isInstanceOf[TBoolean] || typ.isInstanceOf[TNumeric])
  val seqArgs = FastIndexedSeq(typ)
  def resultType = typ
}
final case class ProductAgg(typ: Type) extends PrimAgg {
  assert(typ.isInstanceOf[TInt64] || typ.isInstanceOf[TFloat64])
  val seqArgs = FastIndexedSeq(typ)
  def resultType = typ
}
final case class SumAgg(typ: Type) extends PrimAgg {
  assert(typ.isInstanceOf[TInt64] || typ.isInstanceOf[TFloat64])
  val seqArgs = FastIndexedSeq(typ)
  def resultType = typ
}
final case class TakeAgg(typ: Type) extends PrimAgg {
  val seqArgs = FastIndexedSeq(typ)
  val resultType = TArray(typ)
}
final case class TakeByAgg(value: Type, key: Type) extends PrimAgg {
  val seqArgs = FastIndexedSeq(value, key)
  def resultType = TArray(value)
}
final case class PrevNonnullAgg(typ: Type) extends PrimAgg {
  val seqArgs = FastIndexedSeq(typ)
  def resultType = typ
}

sealed abstract class AggStateType extends BaseType {
  final def pretty(sb: StringBuilder, indent: Int, compact: Boolean): Unit = ???
  def isCommutative: Boolean = this match {
    case PrimAggState(sig) => sig.isCommutative
    case TupleAggState(subAggs) => subAggs.forall(_.isCommutative)
    case ArrayAggState(eltState) => eltState.isCommutative
    case GroupedAggState(_, valueState) => valueState.isCommutative
  }
  def isTrivial: Boolean = this match {
    case TupleAggState(Seq()) => true
    case _ => false
  }
}
case class PrimAggState(op: PrimAgg) extends AggStateType
case class TupleAggState(subAggs: IndexedSeq[AggStateType]) extends AggStateType
case class ArrayAggState(eltState: AggStateType) extends AggStateType
case class GroupedAggState(keyType: Type, valueState: AggStateType) extends AggStateType

sealed abstract class AggInitArgs extends BaseIR {
  def typ: AggStateType
}
case class PrimAggInit(op: PrimAgg, args: IndexedSeq[IR]) extends AggInitArgs {
  assert(args.map(_.typ) == op.seqArgs)
  def typ = PrimAggState(op)
  def children: IndexedSeq[BaseIR] = args
  def copy(newChildren: IndexedSeq[BaseIR]): PrimAggInit =
    PrimAggInit(op, newChildren.map(_.asInstanceOf[IR]))
}
case class TupleAggInit(subAggs: IndexedSeq[AggInitArgs]) extends AggInitArgs {
  def typ = TupleAggState(subAggs.map(_.typ))
  def children: IndexedSeq[BaseIR] = subAggs
  def copy(newChildren: IndexedSeq[BaseIR]): TupleAggInit =
    TupleAggInit(newChildren.map(_.asInstanceOf[AggInitArgs]))
}
case class ArrayAggInit(eltInit: AggInitArgs) extends AggInitArgs {
  def typ = ArrayAggState(eltInit.typ)
  def children = FastIndexedSeq(eltInit)
  def copy(newChildren: IndexedSeq[BaseIR]): ArrayAggInit = newChildren match {
    case Seq(eltInit: AggInitArgs) => ArrayAggInit(eltInit)
  }
}
case class GroupedAggInit(keyType: Type, valueInit: AggInitArgs) extends AggInitArgs {
  def typ = GroupedAggState(keyType, valueInit.typ)
  def children = FastIndexedSeq(valueInit)
  def copy(newChildren: IndexedSeq[BaseIR]): GroupedAggInit = newChildren match {
    case Seq(valueInit: AggInitArgs) => GroupedAggInit(keyType, valueInit)
  }
}

final case class AggType(resType: Type, stateType: AggStateType) extends BaseType {
  def pretty(sb: StringBuilder, indent: Int, compact: Boolean): Unit = ???
}

sealed abstract class AggIR extends BaseIR {
  def typ: AggType
  def resType: Type = typ.resType
  def stateType: AggStateType = typ.stateType
}

case class AggLet2(name: String, value: IR, body: AggIR) extends AggIR {
  def typ = body.typ

  val children = FastIndexedSeq(value, body)

  def copy(newChildren: IndexedSeq[BaseIR]): AggLet2 = newChildren match {
    case Seq(value: IR, body: AggIR) => AggLet2(name, value, body)
  }
}

case class AggDo(
  aggs: IndexedSeq[(Option[String], AggIR)],
  result: IR,
  _stateType: Option[AggStateType] = None
) extends AggIR {
  val typ = AggType(
    resType = result.typ,
    stateType = _stateType match {
                  case None => aggs(0)._2.stateType
                  case Some(state) => state
                })

  require(aggs.forall { case (_, agg) => agg.stateType == stateType })

  val children = aggs.map(_._2) :+ result

  def copy(newChildren: IndexedSeq[BaseIR]): AggDo = {
    val newAggs = aggs.zip(newChildren.init).map {
      case ((name, _), newAgg) => (name, newAgg.asInstanceOf[AggIR])
    }
    AggDo(newAggs, newChildren.last.asInstanceOf[IR], _stateType)
  }
}

case class AggArrayDo(array: IR, eltName: String, body: AggIR, drop: Boolean = false) extends AggIR {
//  val typ = body.typ.copy(resType = if (drop) TVoid else TArray(body.resType))
  val typ = AggType(
    resType = if (drop) TVoid else TArray(body.resType),
    stateType = body.stateType)

  val children = FastIndexedSeq(array, body)

  def copy(newChildren: IndexedSeq[BaseIR]): AggArrayDo = newChildren match {
    case Seq(array: IR, body: AggIR) => AggArrayDo(array, eltName, body, drop)
  }
}

case class AggDoPar(aggs: IndexedSeq[(Option[String], AggIR)], result: IR) extends AggIR {
  val typ = AggType(
    resType = result.typ,
    stateType = TupleAggState(aggs.map(_._2.stateType)))

  val children = aggs.map(_._2) :+ result

  def copy(newChildren: IndexedSeq[BaseIR]): AggDoPar = {
    val newAggs = aggs.zip(newChildren.init).map {
      case ((name, _), newAgg) => (name, newAgg.asInstanceOf[AggIR])
    }
    AggDoPar(newAggs, newChildren.last.asInstanceOf[IR])
  }
}

case class AggArrayDoPar(array: IR, eltName: String, indexName: String, body: AggIR, drop: Boolean = false) extends AggIR {
  val typ = AggType(
    resType = if (drop) TVoid else TArray(body.resType),
    stateType = ArrayAggState(body.stateType))

  val children = FastIndexedSeq(array, body)

  def copy(newChildren: IndexedSeq[BaseIR]): AggArrayDoPar = newChildren match {
    case Seq(array: IR, body: AggIR) => AggArrayDoPar(array, eltName, indexName, body)
  }
}

case class AggPrimSeq(op: PrimAgg, args: IndexedSeq[IR]) extends AggIR {
  require(args.map(_.typ) == op.seqArgs)

  val typ = AggType(resType = TVoid, stateType = PrimAggState(op))

  def children = args

  def copy(newChildren: IndexedSeq[BaseIR]): AggPrimSeq =
    AggPrimSeq(op, newChildren.map(_.asInstanceOf[IR]))
}

case class AggPrimResult(op: PrimAgg) extends AggIR {
  val typ = AggType(resType = op.resultType, stateType = PrimAggState(op))

  val children = FastIndexedSeq()

  def copy(newChildren: IndexedSeq[BaseIR]): AggPrimResult = newChildren match {
    case Seq() => AggPrimResult(op)
  }
}

case class AggGroupedAtKey(key: IR, body: AggIR) extends AggIR {
  val typ = body.typ.copy(stateType = GroupedAggState(key.typ, body.stateType))

  val children = FastIndexedSeq(key, body)

  def copy(newChildren: IndexedSeq[BaseIR]): AggGroupedAtKey = newChildren match {
    case Seq(key: IR, body: AggIR) => AggGroupedAtKey(key, body)
  }
}

case class AggGroupedMap(body: AggIR, drop: Boolean = false, keyType: Type) extends AggIR {
  val typ = AggType(
    resType = if (drop) TVoid else TDict(keyType, body.resType),
    stateType = GroupedAggState(keyType, body.stateType))

  val children = FastIndexedSeq(body)

  def copy(newChildren: IndexedSeq[BaseIR]): AggGroupedMap = newChildren match {
    case Seq(body: AggIR) => AggGroupedMap(body, drop, keyType)
  }
}

object FilterAggIR {
  def apply(cond: IR, aggIR: AggIR): AggIR = ???
}

object SkipAgg {
  def apply(aggIR: AggIR): AggIR = ???
}

object LowerToAggIR {
  def apply(ir: BaseIR): BaseIR = ir match {
    case ir: IR =>
      assert(!ContainsAgg(ir) && !ContainsScan(ir))
      lower(ir)
    case _ => lower(ir)
  }

  private sealed abstract class AggRep {
    def close: Closed
    def open: Open
  }
  private case class Open(aggs: IndexedSeq[(AggInitArgs, String, AggIR)], body: BaseIR) extends AggRep {
    def close: Closed =
      Closed(TupleAggInit(aggs.map(_._1)),
             AggDo(aggs.map(a => (Some(a._2), a._3)), body.asInstanceOf[IR]))
    def open: Open = this
  }
  private case class Closed(init: AggInitArgs, ir: AggIR) extends AggRep {
    def close: Closed = this
    def open: Open = {
      val uid = genUID()
      Open(FastIndexedSeq((init, uid, ir)), Ref(uid, ir.resType))
    }
  }

  private def extract(ir: BaseIR): AggRep = ir match {
    case AggLet(name, value, body, _) =>
      val Closed(init, aggIR) = extract(body).close
      Closed(init, AggLet2(name, value, aggIR))
    case ApplyAggOp(constructorArgs, initOpArgs, seqOpArgs, aggSig) =>
      val op = PrimAgg.fromOldAggOp(aggSig)
      Closed(PrimAggInit(op, constructorArgs ++ initOpArgs.getOrElse(FastIndexedSeq())),
             AggPrimSeq(op, seqOpArgs))
    case ApplyScanOp(constructorArgs, initOpArgs, seqOpArgs, aggSig) =>
      val op = PrimAgg.fromOldAggOp(aggSig)
      Closed(PrimAggInit(op, constructorArgs ++ initOpArgs.getOrElse(FastIndexedSeq())),
             AggDo(FastSeq((Some("res"), AggPrimResult(op)),
                           (None, AggPrimSeq(op, seqOpArgs))),
                   Ref("res", op.resultType)))
    case AggFilter(cond, ir, _) =>
      val Closed(init, aggIR) = extract(ir).close
      Closed(init, FilterAggIR(cond, aggIR))
    case AggExplode(array, name, aggBody, _) =>
      val Closed(init, aggIR) = extract(aggBody).close
      Closed(init,
             AggDo(FastSeq(None -> AggArrayDo(array, name, aggIR, true),
                           Some("res") -> SkipAgg(aggIR)),
                   Ref("res", aggIR.resType)))
    case AggArrayPerElement(a, elementName, indexName, aggBody, knownLength, isScan) =>
      val Closed(init, aggIR) = extract(aggBody).close
      Closed(init, AggArrayDoPar(a, elementName, indexName, aggIR))
    case _ if lowerBase.isDefinedAt(ir) =>
      Open(FastIndexedSeq(), lower(ir).asInstanceOf[IR])
    case _ =>
      val reps = ir.children.map(agg => extract(agg).open)
      val aggs = reps.flatMap(_.aggs)
      Open(aggs, ir.copy(reps.map(_.body)))
  }

  private def lower(ir: BaseIR): BaseIR = RewriteBottomUp(ir, lowerBase.lift)

  private def lowerBase: PartialFunction[BaseIR, BaseIR] = {
    case ArrayAgg(a, name, query) =>
      val Closed(init, aggIR) = extract(AggExplode(a, name, query, isScan = false)).close
      RunAgg(init, aggIR)
    case ArrayAggScan(a, name, query) =>
      val Closed(init, aggIR) = extract(query).close
      RunAgg(init, AggArrayDo(a, name, aggIR))
    case TableAggregate(child, query) =>
      val Closed(init, aggIR) = extract(query).close
      TableAggregateNewAgg(child, init, aggIR)
    case TableMapRows(child, newRow) =>
      val Closed(init, aggIR) = extract(newRow).close
      TableMapRowsNewAgg(child, init, aggIR)
    case TableAggregateByKey(child, expr) =>
      val Closed(init, aggIR) = extract(expr).close
      TableAggregateByKeyNewAgg(child, init, aggIR)
    case TableKeyByAndAggregate(child, expr, newKey, nPartitions, bufferSize) =>
      val Closed(init, aggIR) = extract(expr).close
      TableKeyByAndAggregateNewAgg(child, init, aggIR, newKey, nPartitions, bufferSize)
  }
}