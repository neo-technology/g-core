package algebra.trees

import algebra.exceptions.{JoinCommonReferenceMissingFromEdge, MoreThanOneCommonReferenceForJoinSides, UnequalJoinLabelSetsOnCommonRef}
import algebra.expressions.{Label, Reference}
import algebra.operators.BinaryOperator.reduceLeft
import algebra.operators._
import common.trees.BottomUpRewriter

object NormalizeJoinsOfUnions extends BottomUpRewriter[AlgebraTreeNode] {

  override val rule: RewriteFuncType = {
    case InnerJoin(lhs: UnionAll, rhs: UnionAll, _) => rewriteJoin(lhs, rhs)
    case InnerJoin(lhs, rhs: UnionAll, _) => rewriteJoin(lhs, rhs)
    case InnerJoin(lhs: UnionAll, rhs, _) => rewriteJoin(lhs, rhs)
  }

  private def rewriteJoin(lhs: RelationLike, rhs: RelationLike): RelationLike = {
    val commonRef: Reference = {
      val intersection: Set[Reference] = lhs.getBindingSet.refSet intersect rhs.getBindingSet.refSet
      if (intersection.size > 1) {
        throw MoreThanOneCommonReferenceForJoinSides(lhs, rhs)
      } else {
        intersection.head
      }
    }

    val lhsLabelMap: Map[Label, Seq[RelationLike]] = processSide(lhs, commonRef)
    val rhsLabelMap: Map[Label, Seq[RelationLike]] = processSide(rhs, commonRef)

    val lhsLabelSet: Set[Label] = lhsLabelMap.keySet
    val rhsLabelSet: Set[Label] = rhsLabelMap.keySet
    if (lhsLabelSet != rhsLabelSet)
      throw UnequalJoinLabelSetsOnCommonRef(lhsLabelMap.keySet, rhsLabelMap.keySet)

    reduceLeft(
      relations =
        lhsLabelSet
          .map(label => {
            InnerJoin(
              lhs = reduceLeft(lhsLabelMap(label), UnionAll),
              rhs = reduceLeft(rhsLabelMap(label), UnionAll))
          })
          .toSeq,
      binaryOp = UnionAll)
  }

  private def processSide(relation: RelationLike,
                          commonRef: Reference): Map[Label, Seq[RelationLike]] = {
    relation match {
      case unionAll: UnionAll =>
        val flatUnionAll: Seq[RelationLike] = flattenUnionAll(unionAll)
        val edgeRelations = flatUnionAll.map(_.asInstanceOf[EdgeRelation]) // TODO: Change this.
//        val edgeRelations: Seq[EdgeRelation] =
//          flatUnionAll.collect {
//            case edgeRelation: EdgeRelation => edgeRelation
//          }
//        val nonEdgeRelations: Seq[RelationLike] =
//          flatUnionAll.flatMap {
//            case _: EdgeRelation => None
//            case other => Some(other)
//          }

        // Group by the label of the common reference between the sides of the join.
        edgeRelations.groupBy(edgeRelation => {
          if (edgeRelation.fromRel.ref == commonRef)
            edgeRelation.fromRel.labelRelation.asInstanceOf[Relation].label
          else if (edgeRelation.toRel.ref == commonRef)
            edgeRelation.toRel.labelRelation.asInstanceOf[Relation].label
          else
            throw JoinCommonReferenceMissingFromEdge(commonRef, edgeRelation)
        })

//      case _ => Seq(relation)
    }
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
