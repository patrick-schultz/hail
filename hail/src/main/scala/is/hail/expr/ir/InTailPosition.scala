package is.hail.expr.ir

object InTailPosition {
  def apply(x: IR, i: Int): Boolean = x match {
    case Block(body, _) => i == body.length
    case If(_, _, _) => i != 0
    case _: Switch => i != 0
    case TailLoop(_, params, _, _) => i == params.length
    case _ => false
  }
}
