package is.hail.types.physical.stypes.concrete

import is.hail.annotations.Region
import is.hail.asm4s._
import is.hail.expr.ir.EmitCodeBuilder
import is.hail.types.physical.{PBaseStructCode, PCanonicalNDArray, PCode, PNDArray, PNDArrayCode, PNDArrayValue, PNumeric, PPrimitive, PSettable, PType, PValue}
import is.hail.types.physical.stypes.{SCode, SSettable, SType, SValue}
import is.hail.types.physical.stypes.interfaces.{SNDArray, SNDArrayCode, SNDArrayValue}
import is.hail.utils.FastIndexedSeq

case class SNDArraySlice(pType: PCanonicalNDArray) extends SNDArray {
  def nDims: Int = pType.nDims

  override def elementType: SType = pType.elementType.sType

  def coerceOrCopy(cb: EmitCodeBuilder, region: Value[Region], value: SCode, deepCopy: Boolean): SCode = ???

  def codeTupleTypes(): IndexedSeq[TypeInfo[_]] = FastIndexedSeq(LongInfo)

  def loadFrom(cb: EmitCodeBuilder, region: Value[Region], pt: PType, addr: Code[Long]): SCode = ???

  def fromSettables(settables: IndexedSeq[Settable[_]]): SNDArraySliceSettable = {
    val shape = settables.slice(1, 1 + pType.nDims).asInstanceOf[IndexedSeq[Settable[Long@unchecked]]]
    val strides = settables.slice(1 + pType.nDims, 1 + 2 * pType.nDims).asInstanceOf[IndexedSeq[Settable[Long@unchecked]]]
    val dataFirstElementPointer = settables.last.asInstanceOf[Settable[Long]]
    new SNDArraySliceSettable(this, shape, strides, dataFirstElementPointer)
  }

  def fromCodes(codes: IndexedSeq[Code[_]]): SNDArraySliceCode = {
    val codesT = codes.asInstanceOf[IndexedSeq[Code[Long@unchecked]]]
    val shape = codesT.slice(0, nDims)
    val strides = codesT.slice(nDims, 2*nDims)
    val dataFirstElement = codesT.last
    new SNDArraySliceCode(this, shape, strides, dataFirstElement)
  }

  def canonicalPType(): PType = pType
}

object SNDArraySliceSettable {
  def apply(sb: SettableBuilder, st: SNDArraySlice, name: String): SNDArraySliceSettable = {
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
) extends SNDArrayValue with SSettable {
  val pt: PCanonicalNDArray = st.pType

  def loadElementAddress(indices: IndexedSeq[Value[Long]], cb: EmitCodeBuilder): Code[Long] = {
    assert(indices.size == pt.nDims)
    pt.loadElementFromDataAndStrides(cb, indices, dataFirstElement, strides)
  }

  def loadElement(indices: IndexedSeq[Value[Long]], cb: EmitCodeBuilder): PCode =
    pt.elementType.loadCheapPCode(cb, loadElementAddress(indices, cb))

  def setElement(indices: IndexedSeq[Value[Long]], value: SCode, cb: EmitCodeBuilder): Unit = {
    val eltType = pt.elementType.asInstanceOf[PPrimitive]
    eltType.storePrimitiveAtAddress(cb, loadElementAddress(indices, cb), value)
  }

  def settableTuple(): IndexedSeq[Settable[_]] = shape ++ strides :+ dataFirstElement

  def store(cb: EmitCodeBuilder, v: SCode): Unit = {
    val vSlice = v.asInstanceOf[SNDArraySliceCode]
    shape.zip(vSlice.shape).foreach { case (x, s) => cb.assign(x, s) }
    strides.zip(vSlice.strides).foreach { case (x, s) => cb.assign(x, s) }
    cb.assign(dataFirstElement, vSlice.dataFirstElement)
  }

  override def get: SNDArraySliceCode = new SNDArraySliceCode(st, shape, strides, dataFirstElement)

  override def shapes(cb: EmitCodeBuilder): IndexedSeq[Value[Long]] = shape

  override def strides(cb: EmitCodeBuilder): IndexedSeq[Value[Long]] = strides

  def firstDataAddress(cb: EmitCodeBuilder): Value[Long] = dataFirstElement

  // FIXME: only optimized for column major
  def setToZero(cb: EmitCodeBuilder): Unit = {
    val tmp = cb.mb.newLocal[Long]("NDArray_setToZero_tmp")
    val contiguousDims = cb.mb.newLocal[Int]("NDArray_setToZero_contigDims")

    val eltType = pt.elementType.asInstanceOf[PNumeric with PPrimitive]

    cb.assign(tmp, eltType.byteSize)

    // Find largest prefix of dimensions which are stored contiguously.
    def contigDimsRecur(i: Int): Unit =
      if (i < st.nDims) {
        cb.ifx(tmp.ceq(strides(i)), {
          cb.assign(tmp, tmp * shape(i))
          contigDimsRecur(i+1)
        }, {
          cb.assign(contiguousDims, i+1)
        })
      } else {
        cb.assign(contiguousDims, st.nDims)
      }

    contigDimsRecur(0)

    def recur(startPtr: Code[Long], dim: Int): Unit =
      if (dim > 0) {
        cb.ifx(contiguousDims.ceq(dim), {
          Region.setMemory(startPtr, shape(dim-1) * strides(dim-1), 0: Byte)
        }, {
          val ptr = cb.mb.newLocal[Long](s"NDArray_setToZero_ptr_$dim")
          val end = cb.mb.newLocal[Long](s"NDArray_setToZero_end_$dim")
          cb.assign(ptr, startPtr)
          cb.assign(end, ptr + strides(dim-1) * shape(dim-1))
          cb.forLoop({}, ptr < end, cb.assign(ptr, ptr + strides(dim-1)), recur(ptr, dim - 1))
        })
      } else {
        eltType.storePrimitiveAtAddress(cb, startPtr, PCode(eltType, eltType.zero))
      }

    recur(dataFirstElement, st.nDims)
  }
}

class SNDArraySliceCode(val st: SNDArraySlice, val shape: IndexedSeq[Code[Long]], val strides: IndexedSeq[Code[Long]], val dataFirstElement: Code[Long]) extends SNDArrayCode {
  val pt: PNDArray = st.pType

  override def toPCode(cb: EmitCodeBuilder, region: Value[Region]): PCode = ???

  override def codeTuple(): IndexedSeq[Code[_]] = (dataFirstElement +: shape) ++ strides

  def memoize(cb: EmitCodeBuilder, name: String, sb: SettableBuilder): SNDArrayValue = {
    val s = SNDArraySliceSettable(sb, st, name)
    cb.assign(s, this)
    s
  }

  override def memoize(cb: EmitCodeBuilder, name: String): SNDArrayValue = memoize(cb, name, cb.localBuilder)

  override def memoizeField(cb: EmitCodeBuilder, name: String): SValue = memoize(cb, name, cb.fieldBuilder)

  // Needs stack structs
  override def shape(cb: EmitCodeBuilder): PBaseStructCode = ???
}
