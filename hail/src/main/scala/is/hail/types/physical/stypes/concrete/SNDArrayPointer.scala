package is.hail.types.physical.stypes.concrete

import is.hail.annotations.Region
import is.hail.asm4s._
import is.hail.expr.ir.orderings.CodeOrdering
import is.hail.expr.ir.{EmitCodeBuilder, EmitMethodBuilder, SortOrder}
import is.hail.types.physical.stypes.interfaces.{SNDArray, SNDArrayValue}
import is.hail.types.physical.stypes.{SCode, SType}
import is.hail.types.physical.{PBaseStructCode, PCanonicalNDArray, PCode, PNDArray, PNDArrayCode, PNDArrayValue, PNumeric, PPrimitive, PSettable, PType, PValue}
import is.hail.utils.FastIndexedSeq

case class SNDArrayPointer(pType: PCanonicalNDArray) extends SNDArray {
  def nDims: Int = pType.nDims

  override def elementType: SType = pType.elementType.sType

  def coerceOrCopy(cb: EmitCodeBuilder, region: Value[Region], value: SCode, deepCopy: Boolean): SCode = {
    new SNDArrayPointerCode(this, pType.store(cb, region, value, deepCopy))
  }

  def codeTupleTypes(): IndexedSeq[TypeInfo[_]] = FastIndexedSeq(LongInfo)

  def loadFrom(cb: EmitCodeBuilder, region: Value[Region], pt: PType, addr: Code[Long]): SCode = {
    if (pt == this.pType)
      new SNDArrayPointerCode(this, addr)
    else
      coerceOrCopy(cb, region, pt.loadCheapPCode(cb, addr), deepCopy = false)
  }

  def fromSettables(settables: IndexedSeq[Settable[_]]): SNDArrayPointerSettable = {
    val a = settables(0).asInstanceOf[Settable[Long@unchecked]]
    val shape = settables.slice(1, 1 + pType.nDims).asInstanceOf[IndexedSeq[Settable[Long@unchecked]]]
    val strides = settables.slice(1 + pType.nDims, 1 + 2 * pType.nDims).asInstanceOf[IndexedSeq[Settable[Long@unchecked]]]
    val dataFirstElementPointer = settables.last.asInstanceOf[Settable[Long]]
    assert(a.ti == LongInfo)
    new SNDArrayPointerSettable(this, a, shape, strides, dataFirstElementPointer)
  }

  def fromCodes(codes: IndexedSeq[Code[_]]): SNDArrayPointerCode = {
    val IndexedSeq(a: Code[Long@unchecked]) = codes
    assert(a.ti == LongInfo)
    new SNDArrayPointerCode(this, a)
  }

  def canonicalPType(): PType = pType
}

object SNDArrayPointerSettable {
  def apply(sb: SettableBuilder, st: SNDArrayPointer, name: String): SNDArrayPointerSettable = {
    new SNDArrayPointerSettable(st, sb.newSettable[Long](name),
      Array.tabulate(st.pType.nDims)(i => sb.newSettable[Long](s"${name}_nd_shape_$i")),
      Array.tabulate(st.pType.nDims)(i => sb.newSettable[Long](s"${name}_nd_strides_$i")),
      sb.newSettable[Long](s"${name}_nd_first_element")
    )
  }
}

class SNDArrayPointerSettable(
   val st: SNDArrayPointer,
   val a: Settable[Long],
   val shape: IndexedSeq[Settable[Long]],
   val strides: IndexedSeq[Settable[Long]],
   val dataFirstElement: Settable[Long]
 ) extends PNDArrayValue with PSettable {
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

  def settableTuple(): IndexedSeq[Settable[_]] = FastIndexedSeq(a) ++ shape ++ strides ++ FastIndexedSeq(dataFirstElement)

  def store(cb: EmitCodeBuilder, v: PCode): Unit = {
    cb.assign(a, v.asInstanceOf[SNDArrayPointerCode].a)
    pt.loadShapes(cb, a, shape)
    pt.loadStrides(cb, a, strides)
    cb.assign(dataFirstElement, pt.dataFirstElementPointer(a))
  }

  override def get: PNDArrayCode = new SNDArrayPointerCode(st, a)

  override def shapes(cb: EmitCodeBuilder): IndexedSeq[Value[Long]] = shape

  override def strides(cb: EmitCodeBuilder): IndexedSeq[Value[Long]] = strides

  def firstDataAddress(cb: EmitCodeBuilder): Value[Long] = dataFirstElement
}

class SNDArrayPointerCode(val st: SNDArrayPointer, val a: Code[Long]) extends PNDArrayCode {
  val pt: PCanonicalNDArray = st.pType

  override def code: Code[_] = a

  override def codeTuple(): IndexedSeq[Code[_]] = FastIndexedSeq(a)

  def memoize(cb: EmitCodeBuilder, name: String, sb: SettableBuilder): PNDArrayValue = {
    val s = SNDArrayPointerSettable(sb, st, name)
    cb.assign(s, this)
    s
  }

  override def memoize(cb: EmitCodeBuilder, name: String): PNDArrayValue = memoize(cb, name, cb.localBuilder)

  override def memoizeField(cb: EmitCodeBuilder, name: String): PValue = memoize(cb, name, cb.fieldBuilder)

  override def shape(cb: EmitCodeBuilder): PBaseStructCode = pt.shapeType.loadCheapPCode(cb, pt.representation.loadField(a, "shape"))
}