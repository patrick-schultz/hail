package is.hail.types.physical.stypes.interfaces

import is.hail.asm4s._
import is.hail.expr.ir.EmitCodeBuilder
import is.hail.linalg.{BLAS, LAPACK}
import is.hail.types.physical.{PCanonicalNDArray, PNDArray}
import is.hail.types.physical.stypes.concrete.{SNDArrayPointerCode, SNDArrayPointerSettable, SNDArraySlice, SNDArraySliceCode}
import is.hail.types.physical.stypes.{SCode, SType, SValue}
import is.hail.types.physical.stypes.primitives.{SFloat64Code, SInt64Code}
import is.hail.utils.FastIndexedSeq

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
      // fixme: strides is bytes
      val cond = nds.map(_.strides(cb)(0).cne(1)).reduceLeft[Code[Boolean]](_ || _)
      cb.ifx(cond, cb._fatal("Require column major"))
    }

  def copy(cb: EmitCodeBuilder, _X: SNDArrayCode, _Y: SNDArrayCode): Unit = {
    val X = _X.memoize(cb, "copy_X")
    val Y = _Y.memoize(cb, "copy_Y")
    assertVector(X, Y)
    val n = X.shapes(cb)(0)
    cb.ifx(Y.shapes(cb)(0).cne(n), cb._fatal("copy: vectors have different sizes"))

    Code.invokeScalaObject5[Int, Long, Int, Long, Int, Unit](BLAS.getClass, "dcopy",
      n.toI,
      X.firstDataAddress(cb), X.strides(cb)(0).toI,
      Y.firstDataAddress(cb), Y.strides(cb)(0).toI)
  }

  def scale(cb: EmitCodeBuilder, alpha: SCode, X: SNDArrayCode): Unit =
    scale(cb, alpha.asInstanceOf[SFloat64Code].code, X)

  def scale(cb: EmitCodeBuilder, alpha: Code[Double], _X: SNDArrayCode): Unit = {
    val X = _X.memoize(cb, "copy_X")
    assertVector(X)
    val n = X.shapes(cb)(0)

    Code.invokeScalaObject4[Int, Double, Long, Int, Unit](BLAS.getClass, "dscal",
      n.toI, alpha, X.firstDataAddress(cb), X.strides(cb)(0).toI)
  }

  def gemv(cb: EmitCodeBuilder, trans: String, A: SNDArrayCode, X: SNDArrayCode, Y: SNDArrayCode): Unit = {
    gemv(cb, trans, 1.0, A, X, 1.0, Y)
  }

  def gemv(cb: EmitCodeBuilder, trans: String, alpha: Code[Double], _A: SNDArrayCode, _X: SNDArrayCode, beta: Code[Double], _Y: SNDArrayCode): Unit = {
    val A = _A.memoize(cb, "copy_A")
    val X = _X.memoize(cb, "copy_X")
    val Y = _Y.memoize(cb, "copy_Y")

    assertMatrix(A)
    assertColMajor(cb, A)
    assertVector(X, Y)

    val Seq(m, n) = A.shapes(cb)
    if (trans == "N")
      cb.ifx(X.shapes(cb)(0).cne(n) || Y.shapes(cb)(0).cne(m), cb._fatal("gemv: incompatible dimensions"))
    else
      cb.ifx(X.shapes(cb)(0).cne(m) || Y.shapes(cb)(0).cne(n), cb._fatal("gemv: incompatible dimensions"))

    Code.invokeScalaObject11[String, Int, Int, Double, Long, Int, Long, Int, Double, Long, Int, Unit](BLAS.getClass, "dgemv",
      trans, m.toI, n.toI,
      alpha,
      A.firstDataAddress(cb), A.strides(cb)(1).toI.max(1),
      X.firstDataAddress(cb), X.strides(cb)(0).toI.max(1),
      beta,
      Y.firstDataAddress(cb), Y.strides(cb)(0).toI.max(1))
  }

  def gemm(cb: EmitCodeBuilder, tA: String, tB: String, A: SNDArrayCode, B: SNDArrayCode, C: SNDArrayCode): Unit =
    gemm(cb, tA, tB, 1.0, A, B, 1.0, C)

  def gemm(cb: EmitCodeBuilder, tA: String, tB: String, alpha: Code[Double], _A: SNDArrayCode, _B: SNDArrayCode, beta: Code[Double], _C: SNDArrayCode): Unit = {
    val A = _A.memoize(cb, "copy_A")
    val B = _B.memoize(cb, "copy_B")
    val C = _C.memoize(cb, "copy_C")
    assertMatrix(A, B, C)
    assertColMajor(cb, A, B, C)

    val Seq(a0, a1) = A.shapes(cb)
    val (m, ka) = if (tA == 'N') (a0, a1) else (a1, a0)
    val Seq(b0, b1) = B.shapes(cb)
    val (kb, n) = if (tB == 'N') (b0, b1) else (b1, b0)
    val Seq(c0, c1) = C.shapes(cb)
    cb.ifx(ka.cne(kb) || c0.cne(m) || c1.cne(n), cb._fatal("gemm: incompatible matrix dimensions"))

    Code.invokeScalaObject13[String, String, Int, Int, Int, Double, Long, Int, Long, Int, Double, Long, Int, Unit](BLAS.getClass, "dgemm",
      tA, tB, m.toI, n.toI, ka.toI,
      alpha,
      A.firstDataAddress(cb), A.strides(cb)(1).toI.max(1),
      B.firstDataAddress(cb), B.strides(cb)(1).toI.max(1),
      beta,
      C.firstDataAddress(cb), C.strides(cb)(1).toI.max(1))
  }

  def geqrt(_A: SNDArrayCode, _T: SNDArrayCode, _work: SNDArrayCode, cb: EmitCodeBuilder): Unit = {
    val A = _A.memoize(cb, "copy_A")
    val T = _T.memoize(cb, "copy_T")
    val work = _work.memoize(cb, "copy_work")
    assertMatrix(A, T)
    assertColMajor(cb, A, T)
    assertVector(work)

    val Seq(m, n) = A.shapes(cb)
    val nb = T.shapes(cb)(0)
    cb.ifx(nb > m.min(n) || nb < 1, cb._fatal("invalid block size"))
    cb.ifx(T.shapes(cb)(1).cne(m.min(n)), cb._fatal("invalid T size"))
    cb.ifx(work.shapes(cb)(0) < nb * n, cb._fatal("work array too small"))

    val error = cb.mb.newLocal[Int]()
    cb.assign(error, Code.invokeScalaObject8[Int, Int, Int, Long, Int, Long, Int, Long, Int](LAPACK.getClass, "dgeqrt",
      m.toI, n.toI, nb.toI,
      A.firstDataAddress(cb), A.strides(cb)(1).toI.max(1),
      T.firstDataAddress(cb), T.strides(cb)(1).toI.max(1),
      work.firstDataAddress(cb)))
    cb.ifx(error.cne(0), cb._fatal("LAPACK error dtpqrt. Error code = ", error.toS))
  }

  def gemqrt(side: String, trans: String, _V: SNDArrayCode, _T: SNDArrayCode, _C: SNDArrayCode, _work: SNDArrayCode, cb: EmitCodeBuilder): Unit = {
    val V = _V.memoize(cb, "copy_V")
    val T = _T.memoize(cb, "copy_T")
    val C = _C.memoize(cb, "copy_C")
    val work = _work.memoize(cb, "copy_work")
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
      V.firstDataAddress(cb), V.strides(cb)(1).toI.max(1),
      T.firstDataAddress(cb), T.strides(cb)(1).toI.max(1),
      C.firstDataAddress(cb), C.strides(cb)(1).toI.max(1),
      work.firstDataAddress(cb)))
    cb.ifx(error.cne(0), cb._fatal("LAPACK error dtpqrt. Error code = ", error.toS))
  }

  def tpqrt(_A: SNDArrayCode, _B: SNDArrayCode, _T: SNDArrayCode, _work: SNDArrayCode, cb: EmitCodeBuilder): Unit = {
    val A = _A.memoize(cb, "copy_A")
    val B = _B.memoize(cb, "copy_B")
    val T = _T.memoize(cb, "copy_T")
    val work = _work.memoize(cb, "copy_work")
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
      A.firstDataAddress(cb), A.strides(cb)(1).toI.max(1),
      B.firstDataAddress(cb), B.strides(cb)(1).toI.max(1),
      T.firstDataAddress(cb), T.strides(cb)(1).toI.max(1),
      work.firstDataAddress(cb)))
    cb.ifx(error.cne(0), cb._fatal("LAPACK error dtpqrt. Error code = ", error.toS))
  }

  def tpmqrt(side: String, trans: String, _V: SNDArrayCode, _T: SNDArrayCode, _A: SNDArrayCode, _B: SNDArrayCode, _work: SNDArrayCode, cb: EmitCodeBuilder): Unit = {
    val V = _V.memoize(cb, "copy_V")
    val T = _T.memoize(cb, "copy_T")
    val A = _A.memoize(cb, "copy_A")
    val B = _B.memoize(cb, "copy_B")
    val work = _work.memoize(cb, "copy_work")
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
      V.firstDataAddress(cb), V.strides(cb)(1).toI.max(1),
      T.firstDataAddress(cb), T.strides(cb)(1).toI.max(1),
      A.firstDataAddress(cb), A.strides(cb)(1).toI.max(1),
      B.firstDataAddress(cb), B.strides(cb)(1).toI.max(1),
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
case class ScalarIndex(i: Value[Long]) extends NDArrayIndex
case class SliceIndex(begin: Option[Value[Long]], end: Option[Value[Long]]) extends NDArrayIndex
case object ColonIndex extends NDArrayIndex

trait SNDArrayValue extends SValue {
  def st: SNDArray

  override def get: SNDArrayCode

  def loadElement(indices: IndexedSeq[Value[Long]], cb: EmitCodeBuilder): SCode

  def setToZero(cb: EmitCodeBuilder): Unit

  def loadElementAddress(indices: IndexedSeq[Value[Long]], cb: EmitCodeBuilder): Code[Long]

  def setElement(indices: IndexedSeq[Value[Long]], value: SCode, cb: EmitCodeBuilder): Unit

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

  def slice(cb: EmitCodeBuilder, indices: IndexedSeq[NDArrayIndex]): SNDArraySliceCode = {
    val shapeX = shapes(cb)
    val stridesX = strides(cb)
    val shapeBuilder = mutable.ArrayBuilder.make[Code[Long]]
    val stridesBuilder = mutable.ArrayBuilder.make[Code[Long]]

    for (i <- indices.indices) indices(i) match {
      case ScalarIndex(j) =>
        cb.ifx(j < 0 || j >= shapeX(i), cb._fatal("Index out of bounds"))
      case SliceIndex(Some(begin), Some(end)) =>
        cb.ifx(begin < 0 || end > shapeX(i) || begin > end, cb._fatal("Index out of bounds"))
        shapeBuilder += end - begin
        stridesBuilder += stridesX(i)
      case SliceIndex(None, Some(end)) =>
        cb.ifx(end >= shapeX(i) || end < 0, cb._fatal("Index out of bounds"))
        shapeBuilder += end
        stridesBuilder += stridesX(i)
      case SliceIndex(Some(begin), None) =>
        val end = shapeX(i)
        cb.ifx(begin < 0 || begin > end, cb._fatal("Index out of bounds"))
        shapeBuilder += end - begin
        stridesBuilder += stridesX(i)
      case SliceIndex(None, None) =>
        shapeBuilder += shapeX(i)
        stridesBuilder += stridesX(i)
      case ColonIndex =>
        shapeBuilder += shapeX(i)
        stridesBuilder += stridesX(i)
    }
    val newShape = shapeBuilder.result()
    val newStrides = stridesBuilder.result()

    val firstElementIndices = indices.map {
      case ScalarIndex(j) => j
      case SliceIndex(Some(begin), _) => begin
      case SliceIndex(None, _) => const(0L)
      case ColonIndex => const(0L)
    }

    val newFirstDataAddress = loadElementAddress(firstElementIndices, cb)

    val newSType = SNDArraySlice(PCanonicalNDArray(st.pType.elementType, newShape.size, st.pType.required))

    new SNDArraySliceCode(newSType, newShape, newStrides, newFirstDataAddress)
  }

  def slice(cb: EmitCodeBuilder, indices: Any*): SNDArraySliceCode = {
    val parsedIndices: IndexedSeq[NDArrayIndex] = indices.map {
      case _: ::.type => ColonIndex
      case i: Value[_] => ScalarIndex(i.asInstanceOf[Value[Long]])
      case (begin, end) =>
        val parsedBegin = if (begin == null) None else Some(begin.asInstanceOf[Value[Long]])
        val parsedEnd = if (end == null) None else Some(end.asInstanceOf[Value[Long]])
        SliceIndex(parsedBegin, parsedEnd)
    }.toIndexedSeq
    slice(cb, parsedIndices)
  }
}

trait SNDArrayCode extends SCode {
  def st: SNDArray

  def shape(cb: EmitCodeBuilder): SBaseStructCode

  def memoize(cb: EmitCodeBuilder, name: String): SNDArrayValue
}

object LocalWhitening {
  // Pre: A1 is current window, A2 is next window, [Q1 Q2] R = [A1 A2] is qr fact
  // Post: W contains locally whitened A2, Qout R[-w:, -w:] = A2[:, -w:] is qr fact
  def whiten_base(cb: EmitCodeBuilder,
    Q1: SNDArrayValue, Q2: SNDArrayValue, Qout: SNDArrayValue,
    R: SNDArrayValue, W: SNDArrayValue,
    work1: SNDArrayValue, work2: SNDArrayValue, work3: SNDArrayValue, T: SNDArrayValue
  ): Unit = {
    SNDArray.assertMatrix(Q1, Q2, Qout, R, work1, work2, T)
    SNDArray.assertColMajor(cb, Q1, Q2, Qout, R, work1, work2, T)
    SNDArray.assertVector(work3)

    val Seq(m, w) = Q1.shapes(cb)
    val Seq(blocksize, t1) = T.shapes(cb)
    val Seq(q20, n) = Q2.shapes(cb)
    val Seq(qout0, qout1) = Qout.shapes(cb)
    val Seq(r0, r1) = R.shapes(cb)
    val Seq(w0, w1) = W.shapes(cb)
    val Seq(work10, work11) = work1.shapes(cb)
    val Seq(work20, work21) = work2.shapes(cb)
    val precond =
      n >= w &&
        q20.ceq(m) &&
        qout0.ceq(m) && qout1.ceq(w) &&
        r0.ceq(w+n) && r1.ceq(w+n) &&
        w0.ceq(m) && w1.ceq(n) &&
        work10.ceq(w+n) && work11.ceq(w+n) &&
        work20.ceq(w+n) && work21.ceq(n) &&
        t1.ceq(w+n) &&
        work3.shapes(cb)(0) >= blocksize * (w + n)
    cb.ifx(!precond, cb._fatal("bad inputs to whiten_base"))

    val i = cb.mb.newLocal[Long]("whiten_base_i")

    // set work1 to I
    work1.setToZero(cb)
    cb.forLoop(cb.assign(i, 0L), i.toL < w + n, cb.assign(i, i+1), work1.setElement(FastIndexedSeq(i, i), primitive(1.0), cb))

    cb.forLoop(cb.assign(i, 0L), i < n, cb.assign(i, i+1), {
        // Loop invariant:
        // * ([Q1 Q2] work1[:, i:w+n]) R[i:w+n, i:w+n] = [A1 A2][i:w+n] is qr fact
        // * ([Q1 Q2] work2[:, 1:i]) is locally whitened A2[:, 1:i]

      // work2[:, i] = work[:, w+i] * R[w+i, w+i]
      val wpi = cb.mb.newLocal[Long]("w_plus_i")
      cb.assign(wpi, w + i)
      val w1col = work1.slice(cb, ::, wpi)
      val w2col = work2.slice(cb, ::, i)
      // FIXME: implement memcopy optimization as in SNDArray.setToZero
      SNDArray.copy(cb, w1col, w2col)
      SNDArray.scale(cb, R.loadElement(FastIndexedSeq(wpi, wpi), cb), work2.slice(cb, ::, i))

      val blockToZero = R.slice(cb, (i, i+1), (i+1, null)).memoize(cb, "")
      val Tslice = T.slice(cb, (null, blocksize.min(w+n-i)), (null, w+n-i)).memoize(cb, "")
      SNDArray.tpqrt(R.slice(cb, (i+1, null), (i+1, null)), blockToZero.get, Tslice.get, work3.get, cb)
      SNDArray.tpmqrt("R", "N", blockToZero.get, Tslice.get, work1.slice(cb, ::, (i+1, null)), work1.slice(cb, ::, (i, i+1)), work3.get, cb)
    })

    // W = [Q1 Q2] work2 is locally whitened A2
    SNDArray.gemm(cb, "N", "N", 1.0, Q1.get, work2.slice(cb, (null, w), ::), 0.0, W.get)
    SNDArray.gemm(cb, "N", "N", 1.0, Q2.get, work2.slice(cb, (w, null), ::), 1.0, W.get)

    // Qout = [Q1 Q2] work1, Qout R[n:w+n, n:w+n] = A2[:, n-w:n] is qr fact
    SNDArray.gemm(cb, "N", "N", 1.0, Q1.get, work1.slice(cb, (null, w), ::), 0.0, Qout.get)
    SNDArray.gemm(cb, "N", "N", 1.0, Q2.get, work1.slice(cb, (w, null), ::), 1.0, Qout.get)
  }
}