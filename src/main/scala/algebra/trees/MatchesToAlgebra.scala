package algebra.trees

import algebra.expressions.{AlgebraExpression, Exists, Reference}
import algebra.operators.BinaryOperator.reduceLeft
import algebra.operators._
import common.trees.BottomUpRewriter

import scala.collection.mutable

/**
  * Converts all the remaining [[GcoreOperator]]s in the algebraic tree into
  * [[RelationalOperator]]s in order to obtain a fully relational tree. The entity relations are
  * still used as logical views of their respective tables.
  *
  * A [[CondMatchClause]] and an [[Exists]] sub-clause are transformed into relational trees as
  * follows:
  * - First, [[UnionAll]] subtrees are created from the non-optional [[SimpleMatchRelation]]s that
  * have the same [[BindingSet]].
  * - Next, relations (be they non-optional [[SimpleMatchRelation]]s or [[UnionAll]] relations
  * resulting from step 1) with common bindings in their [[BindingSet]] are [[InnerJoin]]ed.
  * - Relations (be they non-optional [[SimpleMatchRelation]]s or [[InnerJoin]]s from the previous
  * step) with disjoint [[BindingSet]]s are [[CrossJoin]]ed.
  * - Finally, the resulting binding table from the non-optional [[SimpleMatchRelation]]s is
  * [[LeftOuterJoin]]ed with the optional matches, from left to right as they appear in the query
  * (as per the language specification).
  *
  * A [[CondMatchClause]] is then transformed into a [[Select]] clause.
  */
object MatchesToAlgebra extends BottomUpRewriter[AlgebraTreeNode] {

  private val matchClause: RewriteFuncType = {
    case m: MatchClause =>
      reduceLeft(m.children.map(_.asInstanceOf[RelationLike]), LeftOuterJoin)
  }

  private val condMatchClause: RewriteFuncType = {
    case cm: CondMatchClause =>
      val simpleMatches: Seq[SimpleMatchRelation] =
        cm.children.init.map(_.asInstanceOf[SimpleMatchRelation])
      val where: AlgebraExpression = cm.children.last.asInstanceOf[AlgebraExpression]
      val matchesAfterUnion: Seq[RelationLike] = unionSimpleMatchRelations(simpleMatches)
      val joinedMatches: RelationLike = joinSimpleMatchRelations(matchesAfterUnion)
      Select(
        relation = joinedMatches,
        expr = where)
  }

  private val existsClause: RewriteFuncType = {
    case ec: Exists =>
      val simpleMatches: Seq[SimpleMatchRelation] =
        ec.children.map(_.asInstanceOf[SimpleMatchRelation])
      val matchesAfterUnion: Seq[RelationLike] = unionSimpleMatchRelations(simpleMatches)
      val joinedMatches: RelationLike = joinSimpleMatchRelations(matchesAfterUnion)

      ec.children = Seq(joinedMatches)
      ec
  }

  private type RefToRelationsMmap = mutable.HashMap[Set[Reference], mutable.Set[RelationLike]]
    with mutable.MultiMap[Set[Reference], RelationLike]

  private def newRefToRelationsMmap: RefToRelationsMmap =
    new mutable.HashMap[Set[Reference], mutable.Set[RelationLike]]
      with mutable.MultiMap[Set[Reference], RelationLike]

  /** Creates a [[UnionAll]] of the [[RelationLike]]s with the same [[BindingSet]]. */
  private def unionSimpleMatchRelations(relations: Seq[SimpleMatchRelation]): Seq[RelationLike] = {
    relations
      .groupBy(_.getBindingSet.refSet)
      .map {
        case (_, relationsToUnion) => reduceLeft(relationsToUnion, UnionAll)
      }
      .toSeq
  }

  /**
    * [[InnerJoin]]s [[RelationLike]]s with common bindings and [[CrossJoin]]s [[RelationLike]]s
    * with disjoint [[BindingSet]]s.
    */
  private def joinSimpleMatchRelations(relations: Seq[RelationLike]): RelationLike = {
    val refToRelationsMmap: RefToRelationsMmap = newRefToRelationsMmap
    relations.foreach(relation =>
      refToRelationsMmap.addBinding(relation.getBindingSet.refSet, relation))
    val mmapedRelations: RefToRelationsMmap = joinSimpleMatchRelations(refToRelationsMmap)

    reduceLeft(
      relations =
        mmapedRelations.map {
          case (_, relationsToJoin) => reduceLeft(relationsToJoin.toSeq, InnerJoin)
        }.toSeq,
      binaryOp = CrossJoin)
  }

  private def joinSimpleMatchRelations(refToRelationsMmap: RefToRelationsMmap)
  : RefToRelationsMmap = {
    val accumulator: RefToRelationsMmap = newRefToRelationsMmap
    val changed: Boolean = joinSimpleMatchRelations(accumulator, refToRelationsMmap)

    if (changed)
      joinSimpleMatchRelations(accumulator)
    else
      accumulator
  }

  private def joinSimpleMatchRelations(accumulator: RefToRelationsMmap,
                                       refToRelationMmap: RefToRelationsMmap): Boolean = {
    var changed: Boolean = false

    refToRelationMmap.foreach {
      case mmapTuple @ (refSet, relations) =>
        // Find the first key-value tuple in the accumulator for which the Reference set and the
        // mmap Reference set are non-disjoint. This means they have at least one variable in
        // common and should be joined.
        val nonDisjointTuple: Option[(Set[Reference], mutable.Set[RelationLike])] =
          accumulator.collectFirst {
            case accTuple @ (accRefSet, _) if (accRefSet intersect refSet).nonEmpty => accTuple
          }

        if (nonDisjointTuple.isDefined) {
          val accRefSet: Set[Reference] = nonDisjointTuple.get._1
          val accRelations: mutable.Set[RelationLike] = nonDisjointTuple.get._2

          // Remove previous occurrence of the key (set of References).
          accumulator -= accRefSet
          // Add a new key-value pair, where the key is the set union between the mmap Reference set
          // and the accumulator Reference set and the value is the set union of the mmap relation
          // set and the accumulator relation set.
          accumulator += ((accRefSet union refSet) -> (relations union accRelations))

          // A key-value pair in the accumulator has changed.
          changed = true
        } else {
          // There was no key-value pair in the accumulator with a non-disjoint Reference set to
          // this one in the mmap. We add the current tuple to the accumulator.
          accumulator += mmapTuple
        }
    }

    changed
  }

  override val rule: RewriteFuncType = matchClause orElse condMatchClause orElse existsClause
}
