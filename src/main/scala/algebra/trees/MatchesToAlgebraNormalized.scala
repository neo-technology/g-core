package algebra.trees

import algebra.exceptions.{JoinCommonReferenceMissingFromEdge, MoreThanOneCommonReferenceForJoinSides, UnequalJoinLabelSetsOnCommonRef}
import algebra.expressions.{Label, Reference}
import algebra.operators.BinaryOperator.reduceLeft
import algebra.operators._
import common.trees.BottomUpRewriter

import scala.collection.mutable

object MatchesToAlgebraNormalized extends BottomUpRewriter[AlgebraTreeNode] {

  override val rule: RewriteFuncType = {
  }

  private type RefSet = Set[Reference]

  private def simpleMatchesToRelations(relations: Seq[SimpleMatchRelation]): RelationLike = {
    // group relations by common Reference set. These will be relations with the same Reference
    // tuple, but different label tuple.
    val relationsToUnion: Map[RefSet, Seq[SimpleMatchRelation]] =
      relations.groupBy(_.getBindingSet.refSet)

    val joinSequence: Seq[RefSet] =
      relationsToUnion.keys.foldLeft(Seq()) {
        case (agg, refSet) =>

      }
  }
}
