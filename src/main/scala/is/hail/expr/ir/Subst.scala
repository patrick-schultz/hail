package is.hail.expr.ir

object Subst {
  def apply(e: IR): IR = apply(e, Env.empty)
  def apply(e: IR, env: Env[IR]): IR = {
    def subst(e: IR, env: Env[IR] = env): IR = apply(e, env)
    e match {
      case x@Ref(name, _) =>
        env.lookupOption(name).getOrElse(x)
      case Let(name, v, body, _) =>
        Let(name, subst(v), subst(body, env.delete(name)))
      case MapNA(name, v, body, _) =>
        MapNA(name, subst(v), subst(body, env.delete(name)))
      case ArrayMap(a, name, body, _) =>
        ArrayMap(subst(a), name, subst(body, env.delete(name)))
      case ArrayFilter(a, name, cond) =>
        ArrayFilter(subst(a), name, subst(cond, env.delete(name)))
      case ArrayFlatMap(a, name, body) =>
        ArrayFlatMap(subst(a), name, subst(body, env.delete(name)))
      case ArrayFold(a, zero, accumName, valueName, body, _) =>
        ArrayFold(subst(a), subst(zero), accumName, valueName, subst(body, env.delete(accumName).delete(valueName)))
      case ApplyAggOp(a, op, args, typ) =>
        val substitutedArgs = args.map(arg => Recur(subst(_))(arg))
        ApplyAggOp(a, op, substitutedArgs, typ)
      case _ =>
        Recur(subst(_))(e)
    }
  }
}
