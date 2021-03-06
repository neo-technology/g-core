package algebra.target_api

import algebra.expressions.{AlgebraExpression, Label, Reference}
import algebra.operators.{EdgeRelation, Relation}
import algebra.types.Graph
import schema.Catalog

abstract class EdgeScan(edgeRelation: EdgeRelation, graph: Graph, catalog: Catalog) extends
  EntityScan(graph, catalog) {

  val edgeTableName: Label = edgeRelation.labelRelation.asInstanceOf[Relation].label
  val fromTableName: Label = edgeRelation.fromRel.labelRelation.asInstanceOf[Relation].label
  val toTableName: Label = edgeRelation.toRel.labelRelation.asInstanceOf[Relation].label

  val edgeExpr: AlgebraExpression = edgeRelation.expr
  val fromExpr: AlgebraExpression = edgeRelation.fromRel.expr
  val toExpr: AlgebraExpression = edgeRelation.toRel.expr

  val edgeBinding: Reference = edgeRelation.ref
  val fromBinding: Reference = edgeRelation.fromRel.ref
  val toBinding: Reference = edgeRelation.toRel.ref

  children =
    List(edgeBinding, fromBinding, toBinding, edgeTableName, fromTableName, toTableName,
      edgeExpr, fromExpr, toExpr)
}
