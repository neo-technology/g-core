package algebra.expressions

/**
  * An expression between two other [[AlgebraExpression]]s. Usage can be:
  * > lhs symbol rhs
  * or
  * > symbol(lhs, rhs)
  */
abstract class BinaryExpression(lhs: AlgebraExpression, rhs: AlgebraExpression, symbol: String)
  extends AlgebraExpression {

  children = List(lhs, rhs)

  def getSymbol: String = symbol
  def getLhs: AlgebraExpression = lhs
  def getRhs: AlgebraExpression = rhs
}
