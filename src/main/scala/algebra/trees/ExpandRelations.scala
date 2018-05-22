package algebra.trees

import algebra.expressions.{DisjunctLabels, Exists, Label, Reference}
import algebra.operators._
import algebra.types._
import common.trees.TopDownRewriter
import schema.EntitySchema.LabelRestrictionMap
import schema.{Catalog, GraphSchema, SchemaMap}

import scala.collection.mutable

/**
  * A rewriting phase that does label inference on all the variables used in the [[MatchClause]],
  * including:
  * - variables used in the non-optional [[CondMatchClause]];
  * - variables used in the optional [[CondMatchClause]]s (if any);
  * - variables used in the [[Exists]] sub-queries (if any).
  *
  * Label inference is the process of finding the minimal set of [[Label]]s that determine a
  * variable, given all the [[Connection]]s in all the graph patterns of a match clause. In other
  * words, it is the process of identifying which tables will be queried for a particular variable
  * in the [[MatchClause]]. It is essential to find the minimal set of labels/tables, as the runtime
  * of solving a query is directly related to the size of the tables participating in the relational
  * operations.
  *
  * Label inference relies on previous rewrite phases, where entity relations
  * ([[VertexRelation]], [[EdgeRelation]], [[StoredPathRelation]]) have been either labeled with a
  * fixed label ([[Relation]]), if this was provided in the query, or with the label
  * [[AllRelations]], which means that at that point in the rewrite pipeline, we could only infer
  * that the data for the respective variable is to be queried in all available tables for its type.
  *
  * During the inference process, each [[Connection]] in a [[GraphPattern]] becomes a
  * [[SimpleMatchRelation]]. At the end of the inference process:
  * - if a [[Connection]] is strictly labeled (there is only one [[Label]] in the minimal set), then
  * we emit a single [[SimpleMatchRelation]] for that [[Connection]];
  * - otherwise, we emit as many [[SimpleMatchRelation]]s as the size of the minimal set and
  * preserve the binding tuple of the [[Connection]] in the newly formed relations.
  */
case class ExpandRelations(context: AlgebraContext) extends TopDownRewriter[AlgebraTreeNode] {

  sealed abstract class EntityTuple
  sealed case class VertexTuple(label: Label) extends EntityTuple
  sealed case class EdgeOrPathTuple(label: Label, fromLabel: Label, toLabel: Label)
    extends EntityTuple

  private type BindingToLabelsMmap = mutable.HashMap[Reference, mutable.Set[Label]]
    with mutable.MultiMap[Reference, Label]

  private type MatchToBindingTuplesMmap =
    mutable.HashMap[SimpleMatchRelation, mutable.Set[EntityTuple]]
      with mutable.MultiMap[SimpleMatchRelation, EntityTuple]

  private def newBindingToLabelsMmap: BindingToLabelsMmap =
    new mutable.HashMap[Reference, mutable.Set[Label]]
      with mutable.MultiMap[Reference, Label]

  private def newMatchToBindingsMmap: MatchToBindingTuplesMmap =
    new mutable.HashMap[SimpleMatchRelation, mutable.Set[EntityTuple]]
      with mutable.MultiMap[SimpleMatchRelation, EntityTuple]

  override val rule: RewriteFuncType = {
    case matchClause: MatchClause =>
      val catalog: Catalog = context.catalog

      val allSimpleMatches: mutable.ArrayBuffer[SimpleMatchRelation] =
        new mutable.ArrayBuffer[SimpleMatchRelation]()

      // For both non-optional and optional match clauses, replace the SimpleMatchClauses with
      // SimpleMatchRelations and merge all SimpleMatchRelations into the allSimpleMatches buffer.
      // We want to perform label resolution on all patterns as a whole.
      matchClause.children.foreach(
        condMatch => {
          val simpleMatches: Seq[SimpleMatchRelation] =
            toMatchRelations(condMatch.asInstanceOf[CondMatchClause])
          val matchPred: AlgebraTreeNode = condMatch.children.last
          condMatch.children = simpleMatches :+ matchPred

          allSimpleMatches ++= simpleMatches

          // If the matching predicate contains an existence clause, then we need to add to the
          // allSimpleMatches buffer all the SimpleMatchRelations under the Exists condition.
          matchPred forEachDown {
            case e: Exists =>
              val existsMatches: Seq[SimpleMatchRelation] = toMatchRelations(e.children)
              e.children = existsMatches
              allSimpleMatches ++= existsMatches
            case _ =>
          }
        })

      // Use all SimpleMatchRelations to restrict the labels of all variables in the match query.
      val constrainedLabels: BindingToLabelsMmap = restrictLabelsOverall(allSimpleMatches, catalog)

      // Constrain labels for each CondMatchClause.
      matchClause.children.foreach(
        condMatch => {
          val simpleMatches: Seq[SimpleMatchRelation] =
            constrainSimpleMatches(
              simpleMatches = condMatch.children.init, constrainedLabels, catalog)
          val matchPred: AlgebraTreeNode = condMatch.children.last
          condMatch.children = simpleMatches :+ matchPred

          // If the matching predicate is an existence clause, we also constrain the simple matches
          // under the Exists condition.
          matchPred forEachDown {
            case e: Exists =>
              val existsMatches: Seq[SimpleMatchRelation] =
                constrainSimpleMatches(simpleMatches = e.children, constrainedLabels, catalog)
              e.children = existsMatches
            case _ =>
          }
        })

      matchClause
  }

  private def toMatchRelations(condMatchClause: CondMatchClause): Seq[SimpleMatchRelation] = {
    toMatchRelations(condMatchClause.children.init)
  }

  private def toMatchRelations(simpleMatches: Seq[AlgebraTreeNode]): Seq[SimpleMatchRelation] = {
    simpleMatches.flatMap(
      simpleMatchClause => {
        val graphPattern: AlgebraTreeNode = simpleMatchClause.children.head
        val graph: Graph = simpleMatchClause.children.last.asInstanceOf[Graph]
        graphPattern.children.map(pattern =>
          SimpleMatchRelation(
            relation = pattern.asInstanceOf[RelationLike],
            context = SimpleMatchRelationContext(graph)
          ))
      })
  }

  private def constrainSimpleMatches(simpleMatches: Seq[AlgebraTreeNode],
                                     constrainedLabels: BindingToLabelsMmap,
                                     catalog: Catalog): Seq[SimpleMatchRelation] = {
    val constrainedPerMatch: MatchToBindingTuplesMmap =
      restrictLabelsPerMatch(simpleMatches, constrainedLabels, catalog)

    simpleMatches.flatMap {
      case m @ SimpleMatchRelation(rel @ VertexRelation(_, _, _), _, _) =>
        constrainedPerMatch(m).map(tuple =>
          m.copy(
            relation = rel.copy(labelRelation = Relation(tuple.asInstanceOf[VertexTuple].label)))
        )
      case m @ SimpleMatchRelation(rel @ EdgeRelation(_, _, _, fromRel, toRel), _, _) =>
        constrainedPerMatch(m).map(tuple => {
          val edgeTuple: EdgeOrPathTuple = tuple.asInstanceOf[EdgeOrPathTuple]
          m.copy(
            relation = rel.copy(
              labelRelation = Relation(edgeTuple.label),
              fromRel = fromRel.copy(labelRelation = Relation(edgeTuple.fromLabel)),
              toRel = toRel.copy(labelRelation = Relation(edgeTuple.toLabel))))
        })
      case m @ SimpleMatchRelation(
      rel @ StoredPathRelation(_, _, _, _, fromRel, toRel, _, _), _, _) =>
        constrainedPerMatch(m).map(tuple => {
          val pathTuple: EdgeOrPathTuple = tuple.asInstanceOf[EdgeOrPathTuple]
          m.copy(
            relation = rel.copy(
              labelRelation = Relation(pathTuple.label),
              fromRel = fromRel.copy(labelRelation = Relation(pathTuple.fromLabel)),
              toRel = toRel.copy(labelRelation = Relation(pathTuple.toLabel))))
        })
    }
  }

  private def restrictLabelsPerMatch(relations: Seq[AlgebraTreeNode],
                                     constrainedLabels: BindingToLabelsMmap,
                                     catalog: Catalog): MatchToBindingTuplesMmap = {
    val matchToBindingTuplesMmap: MatchToBindingTuplesMmap = newMatchToBindingsMmap
    relations.foreach {
      case relation @ SimpleMatchRelation(VertexRelation(ref, _, _), _, _) =>
        constrainedLabels(ref).foreach(
          label => matchToBindingTuplesMmap.addBinding(relation, VertexTuple(label)))

      case relation @ SimpleMatchRelation(EdgeRelation(edgeRef, _, _, _, _), matchContext, _) =>
        val graphSchema: GraphSchema = extractGraphSchema(matchContext, catalog)
        val constrainedEdgeLabels: Seq[Label] = constrainedLabels(edgeRef).toSeq
        graphSchema.edgeRestrictions.map
          .filter { case (edgeLabel, _) => constrainedEdgeLabels.contains(edgeLabel) }
          .foreach {
            case (edgeLabel, (fromLabel, toLabel)) =>
              matchToBindingTuplesMmap.addBinding(
                relation, EdgeOrPathTuple(edgeLabel, fromLabel, toLabel))
          }

      case relation @ SimpleMatchRelation(
      StoredPathRelation(pathRef, _, _, _, _, _, _, _), matchContext, _) =>
        val graphSchema: GraphSchema = extractGraphSchema(matchContext, catalog)
        val constrainedPathLabels: Seq[Label] = constrainedLabels(pathRef).toSeq
        graphSchema.storedPathRestrictions.map
          .filter { case (pathLabel, _) => constrainedPathLabels.contains(pathLabel) }
          .foreach {
            case (pathLabel, (fromLabel, toLabel)) =>
              matchToBindingTuplesMmap.addBinding(
                relation, EdgeOrPathTuple(pathLabel, fromLabel, toLabel))
          }
    }

    matchToBindingTuplesMmap
  }

  private def restrictLabelsOverall(relations: Seq[SimpleMatchRelation],
                                    catalog: Catalog): BindingToLabelsMmap = {
    val initialRestrictedBindings: BindingToLabelsMmap = newBindingToLabelsMmap
    relations.foreach {
      case SimpleMatchRelation(VertexRelation(ref, _, _), matchContext, _) =>
        val graphSchema: GraphSchema = extractGraphSchema(matchContext, catalog)
        initialRestrictedBindings.update(ref, mutable.Set(graphSchema.vertexSchema.labels: _*))

      case SimpleMatchRelation(EdgeRelation(edgeRef, _, _, fromRel, toRel), matchContext, _) =>
        val graphSchema: GraphSchema = extractGraphSchema(matchContext, catalog)
        val vertexLabels: Seq[Label] = graphSchema.vertexSchema.labels
        initialRestrictedBindings.update(edgeRef, mutable.Set(graphSchema.edgeSchema.labels: _*))
        initialRestrictedBindings.update(fromRel.ref, mutable.Set(vertexLabels: _*))
        initialRestrictedBindings.update(toRel.ref, mutable.Set(vertexLabels: _*))
//
//      case SimpleMatchRelation(VirtualPathRelation(_, _, fromRel, toRel, _, _), matchContext, _) =>
//        val graphSchema: GraphSchema = extractGraphSchema(matchContext, catalog)
//        val vertexLabels: Seq[Label] = graphSchema.vertexSchema.labels
//        initialRestrictedBindings.update(fromRel.ref, mutable.Set(vertexLabels: _*))
//        initialRestrictedBindings.update(toRel.ref, mutable.Set(vertexLabels: _*))

      case SimpleMatchRelation(
      StoredPathRelation(pathRef, _, _, _, fromRel, toRel, _, _), matchContext, _) =>
        val graphSchema: GraphSchema = extractGraphSchema(matchContext, catalog)
        val vertexLabels: Seq[Label] = graphSchema.vertexSchema.labels
        initialRestrictedBindings.update(pathRef, mutable.Set(graphSchema.pathSchema.labels: _*))
        initialRestrictedBindings.update(fromRel.ref, mutable.Set(vertexLabels: _*))
        initialRestrictedBindings.update(toRel.ref, mutable.Set(vertexLabels: _*))
    }

    analyseLabels(relations, catalog, initialRestrictedBindings)
  }

  private def analyseLabels(relations: Seq[SimpleMatchRelation],
                            catalog: Catalog,
                            restrictedBindings: BindingToLabelsMmap): BindingToLabelsMmap = {
    var changed: Boolean = false
    relations.foreach {
      case SimpleMatchRelation(VertexRelation(ref, labelRelation, _), _, _) =>
        changed |= analyseVertexRelation(ref, labelRelation, restrictedBindings)

      case SimpleMatchRelation(EdgeRelation(ref, edgeLblRel, _, fromRel, toRel), matchContext, _) =>
        val graphSchema: GraphSchema = extractGraphSchema(matchContext, catalog)
        changed |=
          analyseEdgeRelation(
            ref, edgeLblRel, fromRel, toRel, graphSchema.edgeRestrictions, restrictedBindings)

      case SimpleMatchRelation(
      StoredPathRelation(ref, _, pathLblRel, _, fromRel, toRel, _, _), matchContext, _) =>
        val graphSchema: GraphSchema = extractGraphSchema(matchContext, catalog)
        changed |=
          analyseStoredPathRelation(
            ref, pathLblRel, fromRel, toRel, graphSchema.storedPathRestrictions, restrictedBindings)

//      case SimpleMatchRelation(VirtualPathRelation(ref, _, fromRel, toRel, _, pathExpr), _, _) =>
//        changed |= analyseVirtualPathRelation(ref, fromRel, toRel, pathExpr, restrictedBindings)
    }

    if (changed)
      analyseLabels(relations, catalog, restrictedBindings)
    else
      restrictedBindings
  }

  private def analyseVertexRelation(ref: Reference,
                                    labelRelation: RelationLike,
                                    restrictedBindings: BindingToLabelsMmap): Boolean = {
    var changed: Boolean = false
    labelRelation match {
      case Relation(label) => changed = tryUpdateStrictLabel(ref, label, restrictedBindings)
      case _ =>
    }
    changed
  }

  private def analyseEdgeRelation(ref: Reference,
                                  labelRelation: RelationLike,
                                  fromRel: VertexRelation,
                                  toRel: VertexRelation,
                                  schemaRestrictions: LabelRestrictionMap,
                                  restrictedBindings: BindingToLabelsMmap): Boolean = {
    analyseEdgeOrStoredPath(
      ref, labelRelation, fromRel, toRel, schemaRestrictions, restrictedBindings)
  }

  private def analyseStoredPathRelation(ref: Reference,
                                        labelRelation: RelationLike,
                                        fromRel: VertexRelation,
                                        toRel: VertexRelation,
                                        schemaRestrictions: LabelRestrictionMap,
                                        restrictedBindings: BindingToLabelsMmap): Boolean = {
    analyseEdgeOrStoredPath(
      ref, labelRelation, fromRel, toRel, schemaRestrictions, restrictedBindings)
  }

//  private def analyseVirtualPathRelation(reference: Reference,
//                                         fromRel: VertexRelation,
//                                         toRel: VertexRelation,
//                                         pathExpression: Option[PathExpression],
//                                         restrictedBindings: BindingToLabelsMmap): Boolean = {
//    var changed: Boolean = false
//    pathExpression match {
//      case Some(KleeneStar(DisjunctLabels(Seq(label)), _, _)) =>
//      case None => _
//    }
//
//    changed
//  }

  private def analyseEdgeOrStoredPath(ref: Reference,
                                      labelRelation: RelationLike,
                                      fromRel: VertexRelation,
                                      toRel: VertexRelation,
                                      schemaRestrictions: LabelRestrictionMap,
                                      restrictedBindings: BindingToLabelsMmap): Boolean = {
    var changed: Boolean = false

    fromRel.labelRelation match {
      // Try to update fromRel.ref binding depending on whether there is any label for fromRel.
      case Relation(label) =>
        changed |= tryUpdateStrictLabel(fromRel.ref, label, restrictedBindings)
      // Try to update fromRel.ref binding depending on the edge bindings.
      case AllRelations =>
        val currentBindings: mutable.Set[Label] = restrictedBindings(fromRel.ref)
        val newBindings: mutable.Set[Label] =
          mutable.Set(
            schemaRestrictions.map
              .filter {
                case (edgeOrPathLabel, _) => restrictedBindings(ref).contains(edgeOrPathLabel)
              }
              .values // (from, to) label tuples
              .map(_._1) // take left endpoint of tuple (from)
              .toSeq: _*)
        val intersection: mutable.Set[Label] = currentBindings intersect newBindings
        if (currentBindings.size != intersection.size) {
          restrictedBindings.update(fromRel.ref, intersection)
          changed = true
        }
    }

    labelRelation match {
      // Try to update this edge's bindings if there is a label assigned to it.
      case Relation(label) => changed |= tryUpdateStrictLabel(ref, label, restrictedBindings)
      // Try to update this edge's bindings based on the bindings of left and right endpoints.
      case AllRelations =>
        val currentBindings: mutable.Set[Label] = restrictedBindings(ref)
        val newBindings: mutable.Set[Label] =
          mutable.Set(
            schemaRestrictions.map
              // Extract the edges that have the left endpoint in the left endpoint's
              // restrictions and the right endpoint in the right endpoint's restrictions.
              .filter {
                case (_, (fromLabel, toLabel)) =>
                  restrictedBindings(fromRel.ref).contains(fromLabel) &&
                    restrictedBindings(toRel.ref).contains(toLabel)
              }
              .keys
              .toSeq: _*)
        val intersection: mutable.Set[Label] = currentBindings intersect newBindings
        if (currentBindings.size != intersection.size) {
          restrictedBindings.update(ref, intersection)
          changed = true
        }
    }

    toRel.labelRelation match {
      // Try to update toRel.ref binding depending on whether there is any label for toRel.
      case Relation(label) => changed |= tryUpdateStrictLabel(toRel.ref, label, restrictedBindings)
      // Try to update toRel.ref binding depending on the edge bindings.
      case AllRelations =>
        val currentBindings: mutable.Set[Label] = restrictedBindings(toRel.ref)
        val newBindings: mutable.Set[Label] =
          mutable.Set(
            schemaRestrictions.map
              .filter {
                case (edgeOrPathLabel, _) => restrictedBindings(ref).contains(edgeOrPathLabel)
              }
              .values // (from, to) label tuples
              .map(_._2) // take right endpoint of tuple (to)
              .toSeq: _*)
        val intersection: mutable.Set[Label] = currentBindings intersect newBindings
        if (currentBindings.size != intersection.size) {
          restrictedBindings.update(toRel.ref, intersection)
          changed = true
        }
    }

    changed
  }

  private def tryUpdateStrictLabel(reference: Reference,
                                   label: Label,
                                   restrictedBindings: BindingToLabelsMmap): Boolean = {
    var changed: Boolean = false
    val currentBindings: mutable.Set[Label] = restrictedBindings(reference)
    val newBinding: mutable.Set[Label] = mutable.Set(label)
    if (!currentBindings.equals(newBinding)) {
      restrictedBindings.update(reference, newBinding)
      changed = true
    }

    changed
  }

  private def extractGraphSchema(matchContext: SimpleMatchRelationContext,
                                 catalog: Catalog): GraphSchema = {
    matchContext.graph match {
      case DefaultGraph => catalog.defaultGraph()
      case NamedGraph(graphName) => catalog.graph(graphName)
    }
  }
}
