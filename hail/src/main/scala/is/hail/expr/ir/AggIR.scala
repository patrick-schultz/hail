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
  val seqArgs = FastIndexedSeq(TFloat64(), TArray(TFloat64()))
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
case class PrimAggState(op: PrimAgg) extends AggStateType {
  def pretty(sb: StringBuilder, indent: Int, compact: Boolean) {
    sb ++= s"PrimAggState<$op>"
  }
}
case class TupleAggState(subAggs: IndexedSeq[AggStateType]) extends AggStateType {
  override def pretty(sb: StringBuilder, indent: Int, compact: Boolean) {
    sb ++= "TupleAggState<"
    subAggs.foreachBetween {
      st => st.pretty(sb, indent, compact)
    }(sb += ',')
    sb += '>'
  }
}
case class ArrayAggState(eltState: AggStateType) extends AggStateType {
  def pretty(sb: StringBuilder, indent: Int, compact: Boolean) {
    sb ++= s"ArrayAggState<$eltState>"
  }
}
case class GroupedAggState(keyType: Type, valueState: AggStateType) extends AggStateType {
  def pretty(sb: StringBuilder, indent: Int, compact: Boolean) {
    sb ++= s"GroupedAggState<$keyType, $valueState>"
  }
}

sealed abstract class AggInitArgs extends BaseIR {
  def typ: AggStateType
}
case class PrimAggInit(op: PrimAgg, args: IndexedSeq[IR]) extends AggInitArgs {
  // FIXME: enable check
//  assert(args.map(_.typ) == op.initArgs)
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
  def pretty(sb: StringBuilder, indent: Int, compact: Boolean) {
      sb.append(s"AggType<$resType, $stateType>")
  }
}

object AggIR {
  def pure(
    stateType: AggStateType = TupleAggState(FastIndexedSeq()),
    value: IR = MakeTuple(FastSeq())
  ): AggIR = AggDo(FastIndexedSeq(), value, stateType)
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

object AggDo {
  def apply(aggs: IndexedSeq[(Option[String], AggIR)], result: IR): AggDo = {
    require(aggs.nonEmpty)
    AggDo(aggs, result, aggs(0)._2.stateType)
  }
}

case class AggDo(
  aggs: IndexedSeq[(Option[String], AggIR)],
  result: IR,
  override val stateType: AggStateType
) extends AggIR {
  val typ = AggType(resType = result.typ, stateType = stateType)

  require(aggs.forall { case (_, agg) => agg.stateType == stateType })

  val children = aggs.map(_._2) :+ result

  def copy(newChildren: IndexedSeq[BaseIR]): AggDo = {
    val newAggs = aggs.zip(newChildren.init).map {
      case ((name, _), newAgg) => (name, newAgg.asInstanceOf[AggIR])
    }
    AggDo(newAggs, newChildren.last.asInstanceOf[IR], stateType)
  }
}

case class AggArrayDo(array: IR, eltName: String, body: AggIR, drop: Boolean = false) extends AggIR {
//  val typ = body.typ.copy(resType = if (drop) TTuple() else TArray(body.resType))
  val typ = AggType(
    resType = if (drop) TTuple() else TArray(body.resType),
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
    resType = if (drop) TTuple() else TArray(body.resType),
    stateType = ArrayAggState(body.stateType))

  val children = FastIndexedSeq(array, body)

  def copy(newChildren: IndexedSeq[BaseIR]): AggArrayDoPar = newChildren match {
    case Seq(array: IR, body: AggIR) => AggArrayDoPar(array, eltName, indexName, body)
  }
}

case class AggPrimSeq(op: PrimAgg, args: IndexedSeq[IR]) extends AggIR {
  require(args.map(_.typ) == op.seqArgs)

  val typ = AggType(resType = TTuple(), stateType = PrimAggState(op))

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

case class AggGroupedWriteKeyReadAll(key: IR, body: AggIR, drop: Boolean = false) extends AggIR {
  val typ = AggType(
    resType = if (drop) TTuple() else TDict(key.typ, body.resType),
    stateType = GroupedAggState(key.typ, body.stateType))

  val children = FastIndexedSeq(key, body)

  def copy(newChildren: IndexedSeq[BaseIR]): AggGroupedWriteKeyReadAll = newChildren match {
    case Seq(key: IR, body: AggIR) => AggGroupedWriteKeyReadAll(key, body, drop)
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
    resType = if (drop) TTuple() else TDict(keyType, body.resType),
    stateType = GroupedAggState(keyType, body.stateType))

  val children = FastIndexedSeq(body)

  def copy(newChildren: IndexedSeq[BaseIR]): AggGroupedMap = newChildren match {
    case Seq(body: AggIR) => AggGroupedMap(body, drop, keyType)
  }
}

object FilterAggIR {
  def apply(cond: IR, aggIR: AggIR): AggIR = {
    assert(cond.typ.isOfType(TBoolean()))
    val newIR = cond match {
      case x: Ref => filter(x, aggIR)
      case _ =>
        val name = genUID()
        AggLet2(name, cond, filter(Ref(name, cond.typ), aggIR))
    }
    assert(newIR.typ == aggIR.typ)
    newIR
  }

  private def filter(cond: Ref, aggIR: AggIR): AggIR = aggIR match {
    case AggLet2(name, value, body) => AggLet2(name, value, filter(cond, body))
    case AggDo(aggs, result, stateType) =>
      AggDo(aggs.map { case (name, aggIR) => (name, filter(cond, aggIR)) },
            result,
            stateType)
    case AggArrayDo(array, eltName, body, drop) =>
      AggArrayDo(array, eltName, filter(cond, body), drop)
    case AggDoPar(aggs, result) =>
      AggDoPar(aggs.map { case (name, aggIR) => (name, filter(cond, aggIR)) },
               result)
    case AggArrayDoPar(array, eltName, indexName, body, drop) =>
      AggArrayDoPar(array, eltName, indexName, filter(cond, body), drop)
    case x: AggPrimSeq => AggIR.pure(x.stateType)
    case x: AggPrimResult => x
    case AggGroupedWriteKeyReadAll(key, body, drop) =>
      AggGroupedWriteKeyReadAll(key, filter(cond, body), drop)
    case AggGroupedAtKey(key, body) => AggGroupedAtKey(key, filter(cond, body))
    case AggGroupedMap(body, drop, keyType) =>
      AggGroupedMap(filter(cond, body), drop, keyType)
  }
}

object SkipAgg {
  def apply(aggIR: AggIR): AggIR = {
    val newIR: AggIR = aggIR match {
      case AggLet2(name, value, body) => AggLet2(name, value, SkipAgg(body))
      case x@AggDo(Seq(), _, _) => x
      case AggDo(aggs, result, stateType) =>
        AggDo(aggs.map { case (name, aggIR) => (name, SkipAgg(aggIR))}, result, stateType)
      case AggDoPar(aggs, result) =>
        AggDoPar(aggs.map { case (name, aggIR) => (name, SkipAgg(aggIR))}, result)
      case AggArrayDoPar(array, eltName, indexName, body, false) =>
        AggArrayDoPar(array, eltName, indexName, SkipAgg(body), false)
      case x: AggPrimResult => x
      case AggGroupedWriteKeyReadAll(key, body, false) =>
        AggGroupedWriteKeyReadAll(key, SkipAgg(body), false)
      case AggGroupedAtKey(key, body) => AggGroupedAtKey(key, SkipAgg(body))
      case AggGroupedMap(body, false, keyType) => AggGroupedMap(SkipAgg(body), false, keyType)
      case _ =>
        assert(aggIR.resType == TTuple())
        AggIR.pure(aggIR.stateType)
    }
    assert(newIR.typ == aggIR.typ)
    newIR
  }
}

object LowerToAggIR {
  def apply(ir: BaseIR): BaseIR = {
    assert(!containsUnboundAgg(ir))
    val origType = ir.typ
    val lowered = ir match {
      case ir: IR =>
        lower(ir)
      case _ => lower(ir)
    }
    assert(origType == lowered.typ)
    lowered
  }

  private def containsUnboundAgg(ir: BaseIR): Boolean = ir match {
    case ir: IR => ir match {
      case _: ApplyAggOp => true
      case _: ApplyScanOp => true
      case _: AggFilter => true
      case _: AggExplode => true
      case _: AggGroupBy => true
      case _: AggArrayPerElement => true
      case _: TableAggregate => false
      case _: MatrixAggregate => false
      case _: ArrayAgg => false
      case _: ArrayAggScan => false
      case _ => ir.children.exists(containsUnboundAgg)
    }
    case _ => false
  }

  private sealed abstract class AggRep {
    def close: Closed
    def open: Open
    def resType: BaseType
  }
  private case class Open(aggs: IndexedSeq[(AggInitArgs, String, AggIR)], body: BaseIR) extends AggRep {
    def close: Closed =
      Closed(TupleAggInit(aggs.map(_._1)),
             AggDoPar(aggs.map(a => (Some(a._2), a._3)), body.asInstanceOf[IR]))
    def open: Open = this
    def resType = body.typ
  }
  private case class Closed(init: AggInitArgs, ir: AggIR) extends AggRep {
    assert(init.typ == ir.stateType)
    def close: Closed = this
    def open: Open = {
      val uid: String = genSym("agg_result").toString
      Open(FastIndexedSeq((init, uid, ir)), Ref(uid, ir.resType))
    }
    def resType = ir.resType
  }

  private def extract(ir: BaseIR): AggRep = {
    ir match {
      case AggLet(name, value, body, _) =>
        val Closed(init, aggIR) = extract(body).close
        val agg = Closed(init, AggLet2(name, value, aggIR))
        assert(agg.resType == ir.typ)
        agg
      case ApplyAggOp(constructorArgs, initOpArgs, seqOpArgs, aggSig) =>
        val op = PrimAgg.fromOldAggOp(aggSig)
        val agg = Closed(PrimAggInit(op, constructorArgs ++ initOpArgs.getOrElse(FastIndexedSeq())),
                         AggDo(FastSeq((Some("res"), AggPrimResult(op)),
                                       (None, AggPrimSeq(op, seqOpArgs))),
                               Ref("res", op.resultType)))
        assert(agg.resType == ir.typ)
        agg
      case ApplyScanOp(constructorArgs, initOpArgs, seqOpArgs, aggSig) =>
        val op = PrimAgg.fromOldAggOp(aggSig)
        val agg = Closed(PrimAggInit(op, constructorArgs ++ initOpArgs.getOrElse(FastIndexedSeq())),
                         AggDo(FastSeq((Some("res"), AggPrimResult(op)),
                                       (None, AggPrimSeq(op, seqOpArgs))),
                               Ref("res", op.resultType)))
        assert(agg.resType == ir.typ)
        agg
      case AggFilter(cond, ir, _) =>
        val Closed(init, aggIR) = extract(ir).close
        val agg = Closed(init, FilterAggIR(cond, aggIR))
        assert(agg.resType == ir.typ)
        agg
      case AggExplode(array, name, aggBody, _) =>
        val Closed(init, aggIR) = extract(aggBody).close
        val agg = Closed(init,
               AggDo(FastSeq(None -> AggArrayDo(array, name, aggIR, true),
                             Some("res") -> SkipAgg(aggIR)),
                     Ref("res", aggIR.resType)))
        assert(agg.resType == ir.typ)
        agg
      case AggGroupBy(key, aggBody, _) =>
        val Closed(init, aggIR) = extract(aggBody).close
        val agg = Closed(GroupedAggInit(key.typ, init),
                         AggGroupedWriteKeyReadAll(key, aggIR))
        assert(agg.resType == ir.typ)
        agg
      case AggArrayPerElement(a, elementName, indexName, aggBody, knownLength, isScan) =>
        val Closed(init, aggIR) = extract(aggBody).close
        val agg = Closed(ArrayAggInit(init), AggArrayDoPar(a, elementName, indexName, aggIR))
        assert(agg.resType == ir.typ)
        agg
      case _ if lowerBase.isDefinedAt(ir) =>
        val agg = Open(FastIndexedSeq(), lower(ir).asInstanceOf[IR])
        assert(agg.resType == ir.typ)
        agg
      case _ =>
        val reps = ir.children.map(agg => extract(agg).open)
        val aggs = reps.flatMap(_.aggs)
        val agg = Open(aggs, ir.copy(reps.map(_.body)))
        assert(agg.resType == ir.typ)
        agg
    }
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