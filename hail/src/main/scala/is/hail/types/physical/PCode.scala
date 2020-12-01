package is.hail.types.physical

import is.hail.annotations.{Region, UnsafeUtils}
import is.hail.asm4s._
import is.hail.expr.ir._
import is.hail.utils._
import is.hail.variant.Genotype

trait PValue { pValueSelf =>
  def pt: PType

  def get(implicit line: LineNumber): PCode

  def value: Value[_] = {
    new Value[Any] {
      override def get(implicit line: LineNumber): Code[Any] = pValueSelf.get.code
    }
  }
}

trait PSettable extends PValue {
  def store(v: PCode)(implicit line: LineNumber): Code[Unit]

  def settableTuple(): IndexedSeq[Settable[_]]

  def load()(implicit line: LineNumber): PCode = get

  def :=(v: PCode)(implicit line: LineNumber): Code[Unit] = store(v)
}

abstract class PCode { self =>
  def pt: PType

  def code: Code[_]

  def codeTuple(): IndexedSeq[Code[_]]

  def typeInfo: TypeInfo[_] = typeToTypeInfo(pt)

  def tcode[T](implicit ti: TypeInfo[T]): Code[T] = {
    assert(ti == typeInfo)
    code.asInstanceOf[Code[T]]
  }

  def store(mb: EmitMethodBuilder[_], r: Value[Region], dst: Code[Long])(implicit line: LineNumber): Code[Unit]

  def allocateAndStore(mb: EmitMethodBuilder[_], r: Value[Region])(implicit line: LineNumber): (Code[Unit], Code[Long]) = {
    val dst = mb.newLocal[Long]()
    (Code(dst := r.allocate(pt.byteSize, pt.alignment), store(mb, r, dst)), dst)
  }

  def asPrimitive: PPrimitiveCode = asInstanceOf[PPrimitiveCode]

  def asIndexable: PIndexableCode = asInstanceOf[PIndexableCode]

  def asBaseStruct: PBaseStructCode = asInstanceOf[PBaseStructCode]

  def asString: PStringCode = asInstanceOf[PStringCode]

  def asInterval: PIntervalCode = asInstanceOf[PIntervalCode]

  def asNDArray: PNDArrayCode = asInstanceOf[PNDArrayCode]

  def asLocus: PLocusCode = asInstanceOf[PLocusCode]

  def asCall: PCallCode = asInstanceOf[PCallCode]

  def asStream: PCanonicalStreamCode = asInstanceOf[PCanonicalStreamCode]

  def castTo(mb: EmitMethodBuilder[_], region: Value[Region], destType: PType, deepCopy: Boolean = false)(implicit line: LineNumber): PCode = {
    PCode(destType,
      destType.copyFromTypeAndStackValue(mb, region, pt, code, deepCopy))
  }

  def copyToRegion(mb: EmitMethodBuilder[_], region: Value[Region], destType: PType = pt)(implicit line: LineNumber): PCode =
    castTo(mb, region, destType, deepCopy = true)

  // this is necessary because Scala doesn't infer the return type of
  // PIndexableCode.memoize if PCode.memoize has a default implementation
  def defaultMemoizeImpl(cb: EmitCodeBuilder, name: String)(implicit line: LineNumber): PValue = {
    new PValue {
      val pt: PType = self.pt

      private val v = cb.newLocalAny(name, code)(typeToTypeInfo(pt), line)

      def get(implicit line: LineNumber): PCode = PCode(pt, v)
    }
  }

  def defaultMemoizeFieldImpl(cb: EmitCodeBuilder, name: String)(implicit line: LineNumber): PValue = {
    new PValue {
      val pt: PType = self.pt

      private val v = cb.newFieldAny(name, code)(typeToTypeInfo(pt), line)

      def get(implicit line: LineNumber): PCode = PCode(pt, v)
    }
  }

  def memoize(cb: EmitCodeBuilder, name: String)(implicit line: LineNumber): PValue

  def memoizeField(cb: EmitCodeBuilder, name: String)(implicit line: LineNumber): PValue
}

object PCode {
  def apply(pt: PType, code: Code[_]): PCode = pt match {
    case pt: PCanonicalArray =>
      new PCanonicalIndexableCode(pt, coerce[Long](code))
    case pt: PCanonicalSet =>
      new PCanonicalIndexableCode(pt, coerce[Long](code))
    case pt: PCanonicalDict =>
      new PCanonicalIndexableCode(pt, coerce[Long](code))
    case pt: PSubsetStruct =>
      new PSubsetStructCode(pt, coerce[Long](code))
    case pt: PCanonicalBaseStruct =>
      new PCanonicalBaseStructCode(pt, coerce[Long](code))
    case pt: PCanonicalBinary =>
      new PCanonicalBinaryCode(pt, coerce[Long](code))
    case pt: PCanonicalString =>
      new PCanonicalStringCode(pt, coerce[Long](code))
    case pt: PCanonicalInterval =>
      new PCanonicalIntervalCode(pt, coerce[Long](code))
    case pt: PCanonicalLocus =>
      new PCanonicalLocusCode(pt, coerce[Long](code))
    case pt: PCanonicalCall =>
      new PCanonicalCallCode(pt, coerce[Int](code))
    case pt: PCanonicalNDArray =>
      new PCanonicalNDArrayCode(pt, coerce[Long](code))
    case pt: PCanonicalStream =>
      throw new UnsupportedOperationException(s"Can't PCode.apply unrealizable PType: $pt")
    case PVoid =>
      throw new UnsupportedOperationException(s"Can't PCode.apply unrealizable PType: $pt")
    case _ =>
      new PPrimitiveCode(pt, code)
  }

  def _empty: PCode = PVoidCode
}

object PSettable {
  def apply(sb: SettableBuilder, _pt: PType, name: String): PSettable = _pt match {
    case pt: PCanonicalArray =>
      PCanonicalIndexableSettable(sb, pt, name)
    case pt: PCanonicalBinary =>
      PCanonicalBinarySettable(sb,pt, name)
    case pt: PCanonicalSet =>
      PCanonicalIndexableSettable(sb, pt, name)
    case pt: PCanonicalDict =>
      PCanonicalIndexableSettable(sb, pt, name)

    case pt: PCanonicalBaseStruct =>
      PCanonicalBaseStructSettable(sb, pt, name)

    case pt: PCanonicalInterval =>
      PCanonicalIntervalSettable(sb, pt, name)
    case pt: PCanonicalLocus =>
      PCanonicalLocusSettable(sb, pt, name)
    case pt: PCanonicalCall =>
      PCanonicalCallSettable(sb, pt, name)

    case _ => new PSettable {
      val pt: PType = _pt

      private val v = sb.newSettable(name)(typeToTypeInfo(pt))

      def settableTuple(): IndexedSeq[Settable[_]] = FastIndexedSeq(v)

      def get(implicit line: LineNumber): PCode = PCode(pt, v)

      def store(pv: PCode)(implicit line: LineNumber): Code[Unit] = {
        v.storeAny(pv.code)
      }
    }
  }
}
