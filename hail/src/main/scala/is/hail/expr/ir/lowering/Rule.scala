package is.hail.expr.ir.lowering

import is.hail.expr.ir._

trait Rule {
  def allows(ir: BaseIR): Boolean
}

case object NoMatrixIR extends Rule {
  def allows(ir: BaseIR): Boolean = !ir.isInstanceOf[MatrixIR]
}

case object NoRelationalLets extends Rule {
  def allows(ir: BaseIR): Boolean = ir match {
    case _: RelationalLet => false
    case _: RelationalLetBlockMatrix => false
    case _: RelationalLetMatrixTable => false
    case _: RelationalLetTable => false
    case _: RelationalRef => false
    case _ => true
  }
}

case object CompilableValueIRs extends Rule {
  def allows(ir: BaseIR): Boolean = ir match {
    case x: IR => Compilable(x)
    case _ => true
  }
}

case object ValueIROnly extends Rule {
  def allows(ir: BaseIR): Boolean = ir match {
    case _: IR => true
    case _ => false
  }
}

case object NoOldAggs extends Rule {
  def allows(ir: BaseIR): Boolean = ir match {
    case _: AggLet => false
    case _: ApplyAggOp => false
    case _: ApplyScanOp => false
    case _: AggFilter => false
    case _: AggExplode => false
    case _: AggGroupBy => false
    case _: AggArrayPerElement => false
    case _: ArrayAgg => false
    case _: ArrayAggScan => false
    case _: TableAggregate => false
    case _: TableMapRows => false
    case _: TableAggregateByKey => false
    case _: TableKeyByAndAggregate => false
    case _: MatrixAggregate => false
    case _: MatrixMapRows => false
    case _: MatrixMapCols => false
    case _: MatrixAggregate => false
    case _: MatrixAggregateRowsByKey => false
    case _: MatrixAggregateColsByKey => false
    case _ => true
  }
}
