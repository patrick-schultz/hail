package is.hail.types.physical.stypes.interfaces

import is.hail.asm4s._
import is.hail.expr.ir.EmitCodeBuilder
import is.hail.linalg.LAPACK
import is.hail.types.physical.PNDArray
import is.hail.types.physical.stypes.concrete.{SNDArrayPointerCode, SNDArrayPointerSettable, SNDArraySliceCode}
import is.hail.types.physical.stypes.{SCode, SType, SValue}
import is.hail.types.physical.stypes.primitives.SInt64Code

import scala.collection.mutable

object SNDArray {
  // Column major order
  def forEachIndex(cb: EmitCodeBuilder, shape: IndexedSeq[Value[Long]], context: String)
    (f: (EmitCodeBuilder, IndexedSeq[Value[Long]]) => Unit): Unit = {

    val indices = Array.tabulate(shape.length) { dimIdx => cb.newLocal[Long](s"${ context }_foreach_dim_$dimIdx", 0L) }

    def recurLoopBuilder(dimIdx: Int, innerLambda: () => Unit): Unit = {
      if (dimIdx == shape.length) {
        innerLambda()
      }
      else {
        val dimVar = indices(dimIdx)

        recurLoopBuilder(dimIdx + 1,
          () => {
            cb.forLoop({
              cb.assign(dimVar, 0L)
            }, dimVar < shape(dimIdx), {
              cb.assign(dimVar, dimVar + 1L)
            },
              innerLambda()
            )
          }
        )
      }
    }

    val body = () => f(cb, indices)

    recurLoopBuilder(0, body)
  }

  // Column major order
  def unstagedForEachIndex(shape: IndexedSeq[Long])
                          (f: IndexedSeq[Long] => Unit): Unit = {

    val indices = Array.tabulate(shape.length) {dimIdx =>  0L}

    def recurLoopBuilder(dimIdx: Int, innerLambda: () => Unit): Unit = {
      if (dimIdx == shape.length) {
        innerLambda()
      }
      else {

        recurLoopBuilder(dimIdx + 1,
          () => {
            (0 until shape(dimIdx).toInt).foreach(_ => {
              innerLambda()
              indices(dimIdx) += 1
            })
          }
        )
      }
    }

    val body = () => f(indices)

    recurLoopBuilder(0, body)
  }

  def assertMatrix(nds: SNDArrayValue*): Unit = {
    for (nd <- nds) assert(nd.st.nDims == 2)
  }

  def assertVector(nds: SNDArrayValue*): Unit = {
    for (nd <- nds) assert(nd.st.nDims == 1)
  }

  def assertColMajor(cb: EmitCodeBuilder, nds: SNDArrayValue*): Unit =
    if (nds.nonEmpty) {
      val cond = nds.map(_.strides(cb)(0).cne(1)).reduceLeft[Code[Boolean]](_ || _)
      cb.ifx(cond, cb._fatal("Require column major"))
    }


  def geqrt(A: SNDArrayValue, T: SNDArrayValue, work: SNDArrayValue, cb: EmitCodeBuilder): Unit = {
    assertMatrix(A, T)
    assertColMajor(cb, A, T)
    assertVector(work)

    val Seq(m, n) = A.shapes(cb)
    val nb = T.shapes(cb)(0)
    cb.ifx(nb > m.min(n) || nb < 1, cb._fatal("invalid block size"))
    cb.ifx(T.shapes(cb)(1) != m.min(n), cb._fatal("invalid T size"))
    cb.ifx(work.shapes(cb)(0) < nb * n, cb._fatal("work array too small"))

    val error = cb.mb.newLocal[Int]()
    cb.assign(error, Code.invokeScalaObject8[Int, Int, Int, Long, Int, Long, Int, Long, Int](LAPACK.getClass, "dgeqrt",
      m.toI, n.toI, nb.toI,
      A.firstDataAddress(cb), A.strides(cb)(1).toI,
      T.firstDataAddress(cb), T.strides(cb)(1).toI,
      work.firstDataAddress(cb)))
    cb.ifx(error.cne(0), cb._fatal("LAPACK error dtpqrt. Error code = ", error.toS))
  }

  def gemqrt(side: String, trans: String, V: SNDArrayValue, T: SNDArrayValue, C: SNDArrayValue, work: SNDArrayValue, cb: EmitCodeBuilder): Unit = {
    assertMatrix(C, V, T)
    assertColMajor(cb, C, V, T)
    assertVector(work)

    assert(side == "L" || side == "R")
    assert(trans == "T" || trans == "N")
    cb.ifx(C.strides(cb)(0).cne(1), cb._fatal("Require column major"))
    cb.ifx(V.strides(cb)(0).cne(1), cb._fatal("Require column major"))
    cb.ifx(T.strides(cb)(0).cne(1), cb._fatal("Require column major"))
    val Seq(l, k) = V.shapes(cb)
    val Seq(m, n) = C.shapes(cb)
    val nb = T.shapes(cb)(0)
    cb.ifx(nb > k || nb < 1, cb._fatal("invalid block size"))
    cb.ifx(T.shapes(cb)(1) != k, cb._fatal("invalid T size"))
    if (side == "L") {
      cb.ifx(l != m, cb._fatal("invalid dimensions"))
      cb.ifx(work.shapes(cb)(0) < nb * n, cb._fatal("work array too small"))
    } else {
      cb.ifx(l != n, cb._fatal("invalid dimensions"))
      cb.ifx(work.shapes(cb)(0) < nb * m, cb._fatal("work array too small"))
    }

    val error = cb.mb.newLocal[Int]()
    cb.assign(error, Code.invokeScalaObject13[String, String, Int, Int, Int, Int, Long, Int, Long, Int, Long, Int, Long, Int](LAPACK.getClass, "dtpmqrt",
      side, trans, m.toI, n.toI, k.toI, nb.toI,
      V.firstDataAddress(cb), V.strides(cb)(1).toI,
      T.firstDataAddress(cb), T.strides(cb)(1).toI,
      C.firstDataAddress(cb), C.strides(cb)(1).toI,
      work.firstDataAddress(cb)))
    cb.ifx(error.cne(0), cb._fatal("LAPACK error dtpqrt. Error code = ", error.toS))
  }

  def tpqrt(A: SNDArrayValue, B: SNDArrayValue, T: SNDArrayValue, work: SNDArrayValue, cb: EmitCodeBuilder): Unit = {
    assertMatrix(A, B, T)
    assertColMajor(cb, A, B, T)
    assertVector(work)

    val Seq(m, n) = B.shapes(cb)
    val nb = T.shapes(cb)(0)
    cb.ifx(nb > n || nb < 1, cb._fatal("invalid block size"))
    cb.ifx(T.shapes(cb)(1) != n, cb._fatal("invalid T size"))
    val Seq(a1, a2) = A.shapes(cb)
    cb.ifx(a1 != n || a2 != n, cb._fatal("invalid A size"))
    cb.ifx(work.shapes(cb)(0) < nb * n, cb._fatal("work array too small"))

    val error = cb.mb.newLocal[Int]()
    cb.assign(error, Code.invokeScalaObject11[Int, Int, Int, Int, Long, Int, Long, Int, Long, Int, Long, Int](LAPACK.getClass, "dtpqrt",
      m.toI, n.toI, 0, nb.toI,
      A.firstDataAddress(cb), A.strides(cb)(1).toI,
      B.firstDataAddress(cb), B.strides(cb)(1).toI,
      T.firstDataAddress(cb), T.strides(cb)(1).toI,
      work.firstDataAddress(cb)))
    cb.ifx(error.cne(0), cb._fatal("LAPACK error dtpqrt. Error code = ", error.toS))
  }

  def tpmqrt(side: String, trans: String, V: SNDArrayValue, T: SNDArrayValue, A: SNDArrayValue, B: SNDArrayValue, work: SNDArrayValue, cb: EmitCodeBuilder): Unit = {
    assertMatrix(A, B, V, T)
    assertColMajor(cb, A, B, V, T)
    assertVector(work)

    assert(side == "L" || side == "R")
    assert(trans == "T" || trans == "N")
    val Seq(l, k) = V.shapes(cb)
    val Seq(m, n) = B.shapes(cb)
    val nb = T.shapes(cb)(0)
    cb.ifx(nb > k || nb < 1, cb._fatal("invalid block size"))
    cb.ifx(T.shapes(cb)(1) != k, cb._fatal("invalid T size"))
    val Seq(a1, a2) = A.shapes(cb)
    if (side == "L") {
      cb.ifx(l != m, cb._fatal("invalid dimensions"))
      cb.ifx(work.shapes(cb)(0) < nb * n, cb._fatal("work array too small"))
      cb.ifx(a1 != k || a2 != n, cb._fatal("A has wrong dimensions"))
    } else {
      cb.ifx(l != n, cb._fatal("invalid dimensions"))
      cb.ifx(work.shapes(cb)(0) < nb * m, cb._fatal("work array too small"))
      cb.ifx(a1 != m || a2 != k, cb._fatal("A has wrong dimensions"))
    }

    val error = cb.mb.newLocal[Int]()
    cb.assign(error, Code.invokeScalaObject16[String, String, Int, Int, Int, Int, Int, Long, Int, Long, Int, Long, Int, Long, Int, Long, Int](LAPACK.getClass, "dtpmqrt",
      side, trans, m.toI, n.toI, k.toI, 0, nb.toI,
      V.firstDataAddress(cb), V.strides(cb)(1).toI,
      T.firstDataAddress(cb), T.strides(cb)(1).toI,
      A.firstDataAddress(cb), A.strides(cb)(1).toI,
      B.firstDataAddress(cb), B.strides(cb)(1).toI,
      work.firstDataAddress(cb)))
    cb.ifx(error.cne(0), cb._fatal("LAPACK error dtpqrt. Error code = ", error.toS))
  }
}


trait SNDArray extends SType {
  def pType: PNDArray

  def nDims: Int

  def elementType: SType
}

sealed abstract class NDArrayIndex
case class ScalarIndex(i: Code[Long]) extends NDArrayIndex
case class SliceIndex(begin: Code[Long], end: Code[Long]) extends NDArrayIndex

trait SNDArrayValue extends SValue {
  def st: SNDArray

  def loadElement(indices: IndexedSeq[Value[Long]], cb: EmitCodeBuilder): SCode

  def shapes(cb: EmitCodeBuilder): IndexedSeq[Value[Long]]

  def strides(cb: EmitCodeBuilder): IndexedSeq[Value[Long]]

  def firstDataAddress(cb: EmitCodeBuilder): Value[Long]

  def outOfBounds(indices: IndexedSeq[Value[Long]], cb: EmitCodeBuilder): Code[Boolean] = {
    val shape = this.shapes(cb)
    val outOfBounds = cb.newLocal[Boolean]("sndarray_out_of_bounds", false)

    (0 until st.nDims).foreach { dimIndex =>
      cb.assign(outOfBounds, outOfBounds || (indices(dimIndex) >= shape(dimIndex)))
    }
    outOfBounds
  }

  def assertInBounds(indices: IndexedSeq[Value[Long]], cb: EmitCodeBuilder, errorId: Int): Code[Unit] = {
    val shape = this.shapes(cb)
    Code.foreach(0 until st.nDims) { dimIndex =>
      val eMsg = const("Index ").concat(indices(dimIndex).toS)
        .concat(s" is out of bounds for axis $dimIndex with size ")
        .concat(shape(dimIndex).toS)
      (indices(dimIndex) >= shape(dimIndex)).orEmpty(Code._fatalWithID[Unit](eMsg, errorId))
    }
  }

  def sameShape(other: SNDArrayValue, cb: EmitCodeBuilder): Code[Boolean] = {
    val otherShapes = other.shapes(cb)
    val b = cb.newLocal[Boolean]("sameShape_b", true)
    val shape = this.shapes(cb)
    assert(shape.length == otherShapes.length)
    shape.zip(otherShapes).foreach { case (s1, s2) =>
      cb.assign(b, b && s1.ceq(s2))
    }
    b
  }

  def slice(cb: EmitCodeBuilder, indices: NDArrayIndex*): SNDArraySliceCode = {
    val newNDims = indices.count(_.isInstanceOf[SliceIndex])

    for (i <- indices) i match {
      case ScalarIndex(i) =>
      case SliceIndex(begin, end) =>
        newShape +=
    }
  }


}

trait SNDArrayCode extends SCode {
  def st: SNDArray

  def shape(cb: EmitCodeBuilder): SBaseStructCode

  def memoize(cb: EmitCodeBuilder, name: String): SNDArrayValue
}

class SNDArraySliceValue(base: SNDArrayValue, ranges: (Value[Long], Value[Long])*) extends SNDArrayValue {
  def st: SNDArray = base.st

  val startIndices: IndexedSeq[Value[Long]] = ranges.map(_._1).toIndexedSeq
  val endIndices: IndexedSeq[Value[Long]] = ranges.map(_._2).toIndexedSeq

  def loadElement(indices: IndexedSeq[Value[Long]], cb: EmitCodeBuilder): SCode =
    base.loadElement(indices.zip(startIndices).map { case (i, s) => cb.memoize(new SInt64Code(true, i+s), "slice_load_element").value.asInstanceOf[Value[Long]] }, cb)

  def shapes(cb: EmitCodeBuilder): IndexedSeq[Value[Long]] =
    ranges.map { case (l, r) => cb.memoize(new SInt64Code(true, r-l), "slice_shape").value.asInstanceOf[Value[Long]] }.toIndexedSeq

  def strides(cb: EmitCodeBuilder): IndexedSeq[Value[Long]] = base.strides(cb)

  def outOfBounds(indices: IndexedSeq[Value[Long]], cb: EmitCodeBuilder): Code[Boolean]

  def assertInBounds(indices: IndexedSeq[Value[Long]], cb: EmitCodeBuilder, errorId: Int = -1): Code[Unit]

  def sameShape(other: SNDArrayValue, cb: EmitCodeBuilder): Code[Boolean]

  def firstDataAddress(cb: EmitCodeBuilder): Value[Long]
}

object LocalWhitening {
  // Pre: A1 is current window, A2 is next window, [Q1 Q2] R = [A1 A2] is qr fact
  // Post: W contains locally whitened A2, Qout R[-w:, -w:] = A2[:, -w:] is qr fact
  def whiten_base(cb: EmitCodeBuilder,
    Q1: SNDArrayValue, Q2: SNDArrayValue, Qout: SNDArrayValue,
    R: SNDArrayValue, W: SNDArrayValue,
    work: SNDArrayValue, work2: SNDArrayValue, T: SNDArrayValue
  ): Unit = {
    SNDArray.assertMatrix(Q1, Q2, Qout, R, T)
    SNDArray.assertColMajor(cb, Q1, Q2, Qout, R, T)
    SNDArray.assertVector(work)

    val Seq(m, w) = Q1.shapes(cb)
    val Seq(blocksize, t1) = T.shapes(cb)
    val Seq(q20, n) = Q2.shapes(cb)
    val Seq(qout0, qout1) = Qout.shapes(cb)
    val Seq(r0, r1) = R.shapes(cb)
    val Seq(w0, w1) = W.shapes(cb)
    val Seq(work0, work1) = work.shapes(cb)
    val precond =
      n >= w &&
        q20 == m &&
        qout0 == m && qout1 == w &&
        r0 == w+n && r1 == w+n &&
        w0 == m && w1 == n &&
        work0 == w+n && work1 == w+n*2 &&
        t1 == w+n &&
        work2.shapes(cb)(0) >= blocksize * (w + n)
    cb.ifx(!precond, cb._fatal("bad inputs to whiten_base"))

    val i = cb.mb.newLocal[Int]("whiten_base_i")
    cb.forLoop(cb.assign(i, 0), i < n.toI, cb.assign(i, i+1), {
        // Loop invariant:
        // * ([Q1 Q2] work[:, i:w+n]) R[i:w+n, i:w+n] = [A1 A2][i:w+n] is qr fact
        // * ([Q1 Q2] work[:, w+n+1:w+n+i]) is locally whitened A2[:, 1:i]

    })
  }
}