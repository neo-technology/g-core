package spark.sql

import algebra.operators._
import algebra.target_api._
import algebra.trees.{AlgebraToTargetTree, AlgebraTreeNode}
import algebra.types.Graph
import algebra.{target_api => target}
import common.RandomNameGenerator.randomString
import compiler.CompileContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import schema.Catalog
import spark.sql.operators._
import spark.sql.{operators => sql}

object SqlPlanner {
  val GROUP_CONSTRUCT_VIEW_PREFIX: String = "GroupConstruct"
}

/** Creates a physical plan with textual SQL queries. */
case class SqlPlanner(compileContext: CompileContext) extends TargetPlanner {

  import spark.sql.SqlPlanner.GROUP_CONSTRUCT_VIEW_PREFIX

  override type StorageType = DataFrame

  val rewriter: AlgebraToTargetTree = AlgebraToTargetTree(compileContext.catalog, this)
  val sparkSession: SparkSession = compileContext.sparkSession
  val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  override def solveBindingTable(matchClause: AlgebraTreeNode): DataFrame = {
    rewriteAndSolveBtableOps(matchClause)
  }

  // TODO: This method needs to return a PathPropertyGraph, built from the currently returned
  // sequence of DataFrames.
  override def constructGraph(btable: DataFrame,
                              constructClauses: Seq[AlgebraTreeNode]): Seq[DataFrame] = {
    // Register the resulting binding table as a view, so that each construct clause can reuse it.
    btable.createOrReplaceGlobalTempView(algebra.trees.BasicToGroupConstruct.BTABLE_VIEW)
    // The root of each tree is a GroupConstruct.

    constructClauses.flatMap(constructClause => {
      val groupConstruct: GroupConstruct = constructClause.asInstanceOf[GroupConstruct]

      // Rewrite the filtered binding table and register it as a global view.
      val baseConstructTableData: DataFrame =
        rewriteAndSolveBtableOps(groupConstruct.getBaseConstructTable)
      baseConstructTableData.createOrReplaceGlobalTempView(groupConstruct.baseConstructViewName)

      if (baseConstructTableData.rdd.isEmpty()) {
        // It can happen that the GroupConstruct filters on contradictory predicates. Example:
        // CONSTRUCT (c) WHEN c.prop > 3, (c)-... WHEN c.prop <= 3 ...
        // In this case, the resulting baseConstructTable will be the empty DataFrame. We should
        // return here the empty DF as well. No other operations on this table will make sense.
        logger.info("The base construct table was empty, cannot build edge table.")
        Seq(sparkSession.emptyDataFrame)
      } else {
        // Rewrite the vertex table.
        val vertexData: DataFrame = rewriteAndSolveBtableOps(groupConstruct.getVertexConstructTable)

        // For the edge table, if it's not the empty relation, register the vertex table as a global
        // view and solve the query to create the edge table.
        val constructData: DataFrame = groupConstruct.getEdgeConstructTable match {
          case RelationLike.empty => vertexData
          case relation @ _ =>
            vertexData.createOrReplaceGlobalTempView(groupConstruct.vertexConstructViewName)
            val vertexAndEdgeData: DataFrame = rewriteAndSolveBtableOps(relation)
            vertexAndEdgeData
        }

        // Register the construct data of this GroupConstruct's as a global view.
        val constructDataViewName: String = s"${GROUP_CONSTRUCT_VIEW_PREFIX}_${randomString()}"
        constructData.createOrReplaceGlobalTempView(constructDataViewName)
        val constructDataTableView: sql.TableView = TableView(constructDataViewName, sparkSession)
        val vertexCreates: Seq[sql.VertexCreate] =
          groupConstruct.createRules
            .collect { case vertexCreate: target.VertexCreate => vertexCreate }
            .map(createRule => sql.VertexCreate(constructDataTableView, createRule))
        val vertexTables: Seq[DataFrame] = vertexCreates.map(solveBtableOps)
        vertexTables
      }
    })
  }

  private def rewriteAndSolveBtableOps(relation: AlgebraTreeNode): DataFrame = {
    val sqlRelation: TargetTreeNode = rewriter.rewriteTree(relation).asInstanceOf[TargetTreeNode]
    logger.info("\n{}", sqlRelation.treeString())
    solveBtableOps(sqlRelation)
  }

  private def solveBtableOps(relation: TargetTreeNode): DataFrame = {
    val btableMetadata: SqlBindingTableMetadata =
      relation.bindingTable.asInstanceOf[SqlBindingTableMetadata]
    val data: DataFrame = btableMetadata.solveBtableOps(sparkSession)
    data.show()
    data
  }

  override def planVertexScan(vertexRelation: VertexRelation, graph: Graph, catalog: Catalog)
  : target.VertexScan = sql.VertexScan(vertexRelation, graph, catalog)

  override def planEdgeScan(edgeRelation: EdgeRelation, graph: Graph, catalog: Catalog)
  : target.EdgeScan = sql.EdgeScan(edgeRelation, graph, catalog)

  override def planPathScan(pathRelation: StoredPathRelation, graph: Graph, catalog: Catalog)
  : target.PathScan = sql.PathScan(pathRelation, graph, catalog)

  override def planUnionAll(unionAllOp: algebra.operators.UnionAll): target.UnionAll =
    sql.UnionAll(
      lhs = unionAllOp.children.head.asInstanceOf[TargetTreeNode],
      rhs = unionAllOp.children.last.asInstanceOf[TargetTreeNode])

  override def planJoin(joinOp: JoinLike): target.Join = {
    val lhs: TargetTreeNode = joinOp.children.head.asInstanceOf[TargetTreeNode]
    val rhs: TargetTreeNode = joinOp.children.last.asInstanceOf[TargetTreeNode]
    joinOp match {
      case _: algebra.operators.InnerJoin => sql.InnerJoin(lhs, rhs)
      case _: algebra.operators.CrossJoin => sql.CrossJoin(lhs, rhs)
      case _: algebra.operators.LeftOuterJoin => sql.LeftOuterJoin(lhs, rhs)
    }
  }

  override def planSelect(selectOp: algebra.operators.Select): target.Select =
    sql.Select(selectOp.children.head.asInstanceOf[TargetTreeNode], selectOp.expr)

  override def createTableView(viewName: String): target.TableView =
    sql.TableView(viewName, sparkSession)

  override def planProject(projectOp: algebra.operators.Project): target.Project =
    sql.Project(projectOp.children.head.asInstanceOf[TargetTreeNode], projectOp.attributes.toSeq)

  override def planGroupBy(groupByOp: algebra.operators.GroupBy): target.GroupBy =
    sql.GroupBy(
      relation = groupByOp.getRelation.asInstanceOf[TargetTreeNode],
      groupingAttributes = groupByOp.getGroupingAttributes,
      aggregateFunctions = groupByOp.getAggregateFunction,
      having = groupByOp.getHaving)

  override def planAddColumn(addColumnOp: algebra.operators.AddColumn): target.AddColumn =
    sql.AddColumn(
      reference = addColumnOp.reference,
      relation =
        sql.Project(
          relation = addColumnOp.children.last.asInstanceOf[TargetTreeNode],
          attributes =
            (addColumnOp.relation.getBindingSet.refSet ++ Set(addColumnOp.reference)).toSeq))

  override def planConstruct(entityConstruct: ConstructRelation)
  : target.EntityConstruct = {

    sql.EntityConstruct(
      reference = entityConstruct.reference,
      isMatchedRef = entityConstruct.isMatchedRef,
      relation = entityConstruct.children(1).asInstanceOf[TargetTreeNode],
      groupedAttributes = entityConstruct.groupedAttributes,
      expr = entityConstruct.expr,
      setClause = entityConstruct.setClause,
      removeClause = entityConstruct.propAggRemoveClause)
  }
}
