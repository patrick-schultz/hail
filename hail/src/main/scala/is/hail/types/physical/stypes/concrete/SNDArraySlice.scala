package is.hail.types.physical.stypes.concrete

import is.hail.annotations.Region
import is.hail.asm4s.{Code, LongInfo, Settable, SettableBuilder, TypeInfo, Value, const}
import is.hail.expr.ir.EmitCodeBuilder
import is.hail.types.physical.{PBaseStructCode, PCanonicalNDArray, PCode, PNDArray, PNDArrayCode, PNDArrayValue, PSettable, PType, PValue}
import is.hail.types.physical.stypes.{SCode, SType}
import is.hail.types.physical.stypes.interfaces.{SNDArray, SNDArrayValue}
import is.hail.utils.FastIndexedSeq

case class SNDArraySlice(pType: PCanonicalNDArray) extends SNDArray {
  def nDims: Int = pType.nDims

  override def elementType: SType = pType.elementType.sType

  def coerceOrCopy(cb: EmitCodeBuilder, region: Value[Region], value: SCode, deepCopy: Boolean): SCode = ???

  def codeTupleTypes(): IndexedSeq[TypeInfo[_]] = FastIndexedSeq(LongInfo)

  def loadFrom(cb: EmitCodeBuilder, region: Value[Region], pt: PType, addr: Code[Long]): SCode = ???

  def fromSettables(settables: IndexedSeq[Settable[_]]): SNDArrayPointerSettable = {
    val a = settables(0).asInstanceOf[Settable[Long@unchecked]]
    val shape = settables.slice(1, 1 + pType.nDims).asInstanceOf[IndexedSeq[Settable[Long@unchecked]]]
    val strides = settables.slice(1 + pType.nDims, 1 + 2 * pType.nDims).asInstanceOf[IndexedSeq[Settable[Long@unchecked]]]
    val dataFirstElementPointer = settables.last.asInstanceOf[Settable[Long]]
    assert(a.ti == LongInfo)
    new SNDArraySliceSettable(this, a, shape, strides, dataFirstElementPointer)
  }

  def fromCodes(codes: IndexedSeq[Code[_]]): SNDArraySliceCode = {
    val IndexedSeq(a: Code[Long@unchecked]) = codes
    assert(a.ti == LongInfo)
    new SNDArraySliceCode(this, a)
  }

  def canonicalPType(): PType = pType
}

object SNDArraySliceSettable {
  def apply(sb: SettableBuilder, st: SNDArray, name: String): SNDArrayPointerSettable = {
    new SNDArraySliceSettable(st,
      Array.tabulate(st.pType.nDims)(i => sb.newSettable[Long](s"${name}_nd_shape_$i")),
      Array.tabulate(st.pType.nDims)(i => sb.newSettable[Long](s"${name}_nd_strides_$i")),
      sb.newSettable[Long](s"${name}_nd_first_element")
    )
  }
}

class SNDArraySliceSettable(
  val st: SNDArraySlice,
  val shape: IndexedSeq[Settable[Long]],
  val strides: IndexedSeq[Settable[Long]],
  val dataFirstElement: Settable[Long]
) extends PNDArrayValue with PSettable {
  val pt: PCanonicalNDArray = st.pType

  def loadElement(indices: IndexedSeq[Value[Long]], cb: EmitCodeBuilder): PCode = {
    assert(indices.size == pt.nDims)
    pt.elementType.loadCheapPCode(cb, pt.loadElementFromDataAndStrides(cb, indices, dataFirstElement, strides))
  }

  def settableTuple(): IndexedSeq[Settable[_]] = shape ++ strides :+ dataFirstElement

  def store(cb: EmitCodeBuilder, v: PCode): Unit = {
    val vSlice = v.asInstanceOf[SNDArraySliceCode]
    shape.zip(vSlice.shape).foreach { case (x, s) => cb.assign(x, s) }
    strides.zip(vSlice.strides).foreach { case (x, s) => cb.assign(x, s) }
    cb.assign(dataFirstElement, vSlice.dataFirstElement)
  }

  override def get: PCode = new SNDArraySliceCode(st, shape, strides, dataFirstElement)

  override def shapes(cb: EmitCodeBuilder): IndexedSeq[Value[Long]] = shape

  override def strides(cb: EmitCodeBuilder): IndexedSeq[Value[Long]] = strides

  def firstDataAddress(cb: EmitCodeBuilder): Value[Long] = dataFirstElement
}

class SNDArraySliceCode(val st: SNDArraySlice, val shape: IndexedSeq[Code[Long]], val strides: IndexedSeq[Code[Long]], val dataFirstElement: Code[Long]) extends PNDArrayCode {
  val pt: PNDArray = st.pType

  override def code: Code[_] = ???

  override def codeTuple(): IndexedSeq[Code[_]] = (dataFirstElement +: shape) ++ strides

  def memoize(cb: EmitCodeBuilder, name: String, sb: SettableBuilder): PNDArrayValue = {
    val s = SNDArraySliceSettable(sb, st, name)
    cb.assign(s, this)
    s
  }

  override def memoize(cb: EmitCodeBuilder, name: String): PNDArrayValue = memoize(cb, name, cb.localBuilder)

  override def memoizeField(cb: EmitCodeBuilder, name: String): PValue = memoize(cb, name, cb.fieldBuilder)

  // Needs stack structs
  override def shape(cb: EmitCodeBuilder): PBaseStructCode = ???
}
