package is.hail.expr.ir.agg

import is.hail.annotations.{Region, StagedRegionValueBuilder}
import is.hail.asm4s._
import is.hail.expr.ir.{EmitClassBuilder, EmitCode, EmitCodeBuilder}
import is.hail.types.physical.{PBooleanRequired, PCanonicalStruct, PFloat64, PInt32Required, PStruct, PType}
import is.hail.io.{BufferSpec, InputBuffer, OutputBuffer}
import is.hail.utils._

class ApproxCDFState(val kb: EmitClassBuilder[_]) extends AggregatorState {
  override val regionSize: Region.Size = Region.TINIER

  private val r: Settable[Region] = kb.genFieldThisRef[Region]()
  val region: Value[Region] = r

  val storageType: PStruct = PCanonicalStruct(true, ("id", PInt32Required), ("initialized", PBooleanRequired), ("k", PInt32Required))
  private val aggr = kb.genFieldThisRef[ApproxCDFStateManager]("aggr")

  private val initialized = kb.genFieldThisRef[Boolean]("initialized")
  private def initializedOffset(off: Code[Long])(implicit line: LineNumber): Code[Long] =
    storageType.loadField(off, "initialized")

  private val id = kb.genFieldThisRef[Int]("id")
  private def idOffset(off: Code[Long])(implicit line: LineNumber): Code[Long] =
    storageType.loadField(off, "id")

  private val k = kb.genFieldThisRef[Int]("k")
  private def kOffset(off: Code[Long])(implicit line: LineNumber): Code[Long] =
    storageType.loadField(off, "k")

  def init(k: Code[Int])(implicit line: LineNumber): Code[Unit] = {
    Code(
      this.k := k,
      aggr := Code.newInstance[ApproxCDFStateManager, Int](this.k),
      id := region.storeJavaObject(aggr),
      this.initialized := true
    )
  }

  def seq(x: Code[Double])(implicit line: LineNumber): Code[Unit] = {
    aggr.invoke[Double, Unit]("seqOp", x)
  }

  def comb(other: ApproxCDFState)(implicit line: LineNumber): Code[Unit] = {
    aggr.invoke[ApproxCDFStateManager, Unit]("combOp", other.aggr)
  }

  def result(srvb: StagedRegionValueBuilder)(implicit line: LineNumber): Code[Unit] = {
    srvb.addIRIntermediate(QuantilesAggregator.resultType)(aggr.invoke[Region, Long]("rvResult", srvb.region))
  }

  def newState(off: Code[Long])(implicit line: LineNumber): Code[Unit] =
    region.getNewRegion(regionSize)

  def createState(cb: EmitCodeBuilder): Unit = {
    implicit val line = cb.lineNumber
    cb.ifx(region.isNull, cb.assign(r, Region.stagedCreate(regionSize)))
  }

  override def load(regionLoader: Value[Region] => Code[Unit], src: Code[Long])(implicit line: LineNumber): Code[Unit] =
    Code.memoize(src, "acdfa_load_src") { src =>
      Code(
        regionLoader(r),
        id := Region.loadInt(idOffset(src)),
        initialized := Region.loadBoolean(initializedOffset(src)),
        initialized.orEmpty(Code(
          aggr := Code.checkcast[ApproxCDFStateManager](region.lookupJavaObject(id)),
          k := Region.loadInt(kOffset(src)))))
    }

  override def store(regionStorer: Value[Region] => Code[Unit], dest: Code[Long])(implicit line: LineNumber): Code[Unit] =
    Code.memoize(dest, "acdfa_store_dest") { dest =>
      region.isValid.orEmpty(
        Code(
          regionStorer(region),
          region.invalidate(),
          Region.storeInt(idOffset(dest), id),
          Region.storeInt(kOffset(dest), k),
          Region.storeBoolean(initializedOffset(dest), initialized)))
    }

  override def serialize(codec: BufferSpec)(implicit line: LineNumber): (EmitCodeBuilder, Value[OutputBuffer]) => Unit = {
    (cb, ob: Value[OutputBuffer]) =>
      cb += Code(
        ob.writeBoolean(initialized),
        ob.writeInt(k),
        initialized.orEmpty(
          aggr.invoke[OutputBuffer, Unit]("serializeTo", ob)
        ))
  }

  override def deserialize(codec: BufferSpec)(implicit line: LineNumber): (EmitCodeBuilder, Value[InputBuffer]) => Unit = {
    (cb, ib: Value[InputBuffer]) =>
      cb += Code(
        initialized := ib.readBoolean(),
        k := ib.readInt(),
        initialized.orEmpty(
          Code(
            aggr := Code.invokeScalaObject2[Int, InputBuffer, ApproxCDFStateManager](
              ApproxCDFStateManager.getClass, "deserializeFrom", k, ib),
            id := region.storeJavaObject(aggr)
          )
        ))
  }

  override def copyFrom(cb: EmitCodeBuilder, src: Code[Long]): Unit = {
    implicit val line = cb.lineNumber
    cb += Code(
      k := Region.loadInt(kOffset(src)),
      aggr := Code.newInstance[ApproxCDFStateManager, Int](k),
      id := region.storeJavaObject(aggr),
      this.initialized := true
    )
  }
}

class ApproxCDFAggregator extends StagedAggregator {
  type State = ApproxCDFState

  def resultType: PStruct = QuantilesAggregator.resultType
  val initOpTypes: Seq[PType] = FastSeq(PInt32Required)
  val seqOpTypes: Seq[PType] = FastSeq(PFloat64())

  protected def _initOp(cb: EmitCodeBuilder, state: State, init: Array[EmitCode]): Unit = {
    implicit val line = cb.lineNumber
    val Array(k) = init
    cb += Code(
      k.setup,
      k.m.mux(
        Code._fatal[Unit]("approx_cdf: 'k' may not be missing"),
        state.init(k.v.asInstanceOf[Code[Int]])
      ))
  }

  protected def _seqOp(cb: EmitCodeBuilder, state: State, seq: Array[EmitCode]): Unit = {
    implicit val line = cb.lineNumber
    val Array(x) = seq
    cb += Code(
      x.setup,
      x.m.mux(
        Code._empty,
        state.seq(x.v.asInstanceOf[Code[Double]])
      ))
  }

  protected def _combOp(cb: EmitCodeBuilder, state: State, other: State): Unit = {
    implicit val line = cb.lineNumber
    cb += state.comb(other)
  }

  protected def _result(cb: EmitCodeBuilder, state: State, srvb: StagedRegionValueBuilder): Unit = {
    implicit val line = cb.lineNumber
    cb += state.result(srvb)
  }
}
