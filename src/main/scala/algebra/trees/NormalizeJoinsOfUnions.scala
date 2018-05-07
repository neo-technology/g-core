package algebra.trees

import algebra.expressions.Reference
import algebra.operators.{InnerJoin, RelationLike, UnionAll}
import common.trees.BottomUpRewriter

object NormalizeJoinsOfUnions extends BottomUpRewriter[AlgebraTreeNode] {

  override val rule: RewriteFuncType = {
    case InnerJoin(lhs: UnionAll, rhs: UnionAll, _) => rewriteJoin(lhs, rhs)
    case InnerJoin(lhs, rhs: UnionAll, _) => rewriteJoin(lhs, rhs)
    case InnerJoin(lhs: UnionAll, rhs, _) => rewriteJoin(lhs, rhs)
  }

  private def rewriteJoin(lhs: RelationLike, rhs: RelationLike): UnionAll = {
    val flatLhs: Seq[RelationLike] = lhs match {
      case unionAll: UnionAll => flattenUnionAll(unionAll)
      case _ => Seq(lhs)
    }
    val flatRhs: Seq[RelationLike] = rhs match {
      case unionAll: UnionAll => flattenUnionAll(unionAll)
      case _ => Seq(rhs)
    }

    // TODO:
    // If either side only contains EdgeRelations, then we need to union those that have the label
    // of the destination/source in common, but different edge labels.
    // After this, we need to join the relations that have the label of the destination + relations
    // that have the label of the source in common.
    // Over the result, we apply a UnionAll.
  }

  private def flattenUnionAll(unionAll: UnionAll): Seq[RelationLike] = {
    unionAll match {
      case UnionAll(lhs: UnionAll, rhs: UnionAll, _) => flattenUnionAll(lhs) ++ flattenUnionAll(rhs)
      case UnionAll(lhs, rhs: UnionAll, _) => lhs +: flattenUnionAll(rhs)
      case UnionAll(lhs: UnionAll, rhs, _) => flattenUnionAll(lhs) :+ rhs
      case UnionAll(lhs, rhs, _) => Seq(lhs, rhs)
    }
  }
}
