package algebra.exceptions

import algebra.expressions.{Label, Reference}
import algebra.operators.{CrossJoin, EdgeRelation, RelationLike}
import algebra.types.ConnectionConstruct

abstract class AnalysisException(reason: String) extends AlgebraException(reason)

/**
  * Two relations can only be joined if they contain at least one common binding. This is not an
  * exception thrown due to an invalid query, but rather due to errors in the rewriting stages. The
  * rule applies to all available joins in the algebra, except for the [[CrossJoin]].
  */
case class JoinException(lhsBset: Set[Reference], rhsBset: Set[Reference])
  extends AnalysisException(
    s"Cannot join relations with no common attributes. Left attributes are: $lhsBset, right " +
      s"attributes are $rhsBset")

/**
  * Two relations can only be [[CrossJoin]]ed (cartesian product) if they share no common bindings.
  * Otherwise any other type of join should be used, depending on the semantics. This is not an
  * exception thrown due to an invalid query, but rather due to errors in the rewriting stages.
  */
case class CrossJoinException(lhsBset: Set[Reference], rhsBset: Set[Reference])
  extends AnalysisException(
    s"Cannot cross-join relations with common attributes. Left attributes are: $lhsBset, right " +
      s"attributes are $rhsBset")

/** Ambiguous features of two [[ConnectionConstruct]]s to be merged. */
case class AmbiguousMerge(reason: String) extends AnalysisException(reason)

case class JoinCommonReferenceMissingFromEdge(commonReference: Reference,
                                              edgeRelation: EdgeRelation)
  extends AnalysisException(
    s"The common reference ${commonReference.refName} is missing from the edge relation: " +
      s"(${edgeRelation.fromRel.ref.refName})-[${edgeRelation.ref.refName}]->" +
      s"(${edgeRelation.toRel.ref.refName}).")

case class MoreThanOneCommonReferenceForJoinSides(lhs: RelationLike, rhs: RelationLike)
  extends AnalysisException(
    "Two joined relations have more than one common variable between them. The binding sets are: " +
      s"lhs = ${lhs.getBindingSet.refSet.map(_.refName)}, " +
      s"rhs = ${rhs.getBindingSet.refSet.map(_.refName)}. The common variables are: " +
      s"${(lhs.getBindingSet.refSet intersect rhs.getBindingSet.refSet).map(_.refName)}.")

case class UnequalJoinLabelSetsOnCommonRef(lhsSet: Set[Label], rhsSet: Set[Label])
  extends AnalysisException(
    "Trying to join two sides with unequal label sets. The labels sets are: " +
      s"lhs = ${lhsSet.map(_.value)}, rhs = ${rhsSet.map(_.value)}.")
