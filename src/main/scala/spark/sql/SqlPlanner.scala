package spark.sql

import algebra.expressions.{Label, Reference}
import algebra.operators._
import algebra.operators.Column.tableLabelColumn
import algebra.target_api._
import algebra.trees.{AlgebraToTargetTree, AlgebraTreeNode}
import algebra.types.{Graph, InConn, OutConn}
import algebra.{target_api => target}
import common.RandomNameGenerator.randomString
import compiler.CompileContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import schema.EntitySchema.LabelRestrictionMap
import schema._
import spark.SparkGraph
import spark.sql.SqlPlanner.{ConstructClauseData, GRAPH_NAME_LENGTH, GROUP_CONSTRUCT_VIEW_PREFIX}
import spark.sql.operators._
import spark.sql.{operators => sql}

object SqlPlanner {
  val GRAPH_NAME_LENGTH: Int = 8
  val GROUP_CONSTRUCT_VIEW_PREFIX: String = "GroupConstruct"

  sealed case class ConstructClauseData(vertexDataMap: Map[Reference, Table[DataFrame]],
                                        edgeDataMap: Map[Reference, Table[DataFrame]],
                                        edgeRestrictions: LabelRestrictionMap)
}

/** Creates a physical plan with textual SQL queries. */
case class SqlPlanner(compileContext: CompileContext) extends TargetPlanner {

  override type StorageType = DataFrame

  val rewriter: AlgebraToTargetTree = AlgebraToTargetTree(compileContext.catalog, this)
  val sparkSession: SparkSession = compileContext.sparkSession
  val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  override def solveBindingTable(matchClause: AlgebraTreeNode): DataFrame =
    rewriteAndSolveBtableOps(matchClause)

  override def constructGraph(btable: DataFrame,
                              constructClauses: Seq[AlgebraTreeNode]): PathPropertyGraph = {
    // Register the resulting binding table as a view, so that each construct clause can reuse it.
    btable.createOrReplaceGlobalTempView(algebra.trees.BasicToGroupConstruct.BTABLE_VIEW)

    // Create a ConstructClauseData from each available construct clause.
    val constructClauseData: Seq[ConstructClauseData] = constructClauses.map(solveConstructClause)

    // Union all data from construct clauses into a single PathPropertyGraph.
    val graph: SparkGraph = new SparkGraph {
      override def graphName: String = randomString(length = GRAPH_NAME_LENGTH)

      override def storedPathRestrictions: LabelRestrictionMap = SchemaMap.empty

      override def edgeRestrictions: LabelRestrictionMap =
        constructClauseData.map(_.edgeRestrictions).reduce(_ union _)

      override def pathData: Seq[Table[DataFrame]] = Seq.empty

      override def vertexData: Seq[Table[DataFrame]] =
        constructClauseData.map(_.vertexDataMap).reduce(_ ++ _).values.toSeq

      override def edgeData: Seq[Table[DataFrame]] =
        constructClauseData.map(_.edgeDataMap).reduce(_ ++ _).values.toSeq
    }
    logger.info(s"Constructed new graph:\n$graph")

    graph
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

  private def rewriteAndSolveBtableOps(relation: AlgebraTreeNode): DataFrame = {
    val sqlRelation: AlgebraTreeNode = rewriter.rewriteTree(relation)
    solveBtableOps(sqlRelation)
  }

  private def solveBtableOps(relation: AlgebraTreeNode): DataFrame = {
    logger.info("\nSolving\n{}", relation.treeString())
    val btableMetadata: SqlBindingTableMetadata =
      relation.asInstanceOf[TargetTreeNode].bindingTable.asInstanceOf[SqlBindingTableMetadata]
    val data: DataFrame = btableMetadata.solveBtableOps(sparkSession)
    data.show()
    data
  }

  private def solveConstructClause(constructClause: AlgebraTreeNode): ConstructClauseData = {
    // The root of each tree is a GroupConstruct.
    val groupConstruct: GroupConstruct = constructClause.asInstanceOf[GroupConstruct]

    // Rewrite the filtered binding table and register it as a global view.
    val baseConstructTableData: DataFrame =
      rewriteAndSolveBtableOps(groupConstruct.getBaseConstructTable)
    baseConstructTableData.createOrReplaceGlobalTempView(groupConstruct.baseConstructViewName)

    if (baseConstructTableData.rdd.isEmpty()) {
      // It can happen that the GroupConstruct filters on contradictory predicates. Example:
      // CONSTRUCT (c) WHEN c.prop > 3, (c)-[]-... WHEN c.prop <= 3 ...
      // In this case, the resulting baseConstructTable will be the empty DataFrame. We should
      // return here the empty DF as well. No other operations on this table will make sense.
      logger.info("The base construct table was empty, cannot build edge table.")
      ConstructClauseData(
        vertexDataMap = Map.empty,
        edgeDataMap = Map.empty,
        edgeRestrictions = SchemaMap.empty)
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

      // Rewrite the CreateRules, and then construct each entity in turn.
      val targetCreateRules: Seq[AlgebraTreeNode] =
        groupConstruct.createRules.map(rule => rewriter.rewriteTree(rule))
      val vertexSqlCreates: Seq[sql.VertexCreate] =
        targetCreateRules
          .collect { case vertexCreate: target.VertexCreate => vertexCreate }
          .map(createRule => sql.VertexCreate(constructDataTableView, createRule))
      val vertexRefToCreateRuleMap: Map[Reference, target.VertexCreate] =
        vertexSqlCreates
          .map(vertexCreate => vertexCreate.createRule.reference -> vertexCreate.createRule)
          .toMap
      val edgeSqlCreates: Seq[sql.EdgeCreate] =
        targetCreateRules
          .collect { case edgeCreate: target.EdgeCreate => edgeCreate }
          .map(createRule =>
            sql.EdgeCreate(
              constructDataTableView,
              createRule,
              vertexRefToCreateRuleMap(createRule.leftReference),
              vertexRefToCreateRuleMap(createRule.rightReference)))

      val vertexRefToDataMap: Map[Reference, Table[DataFrame]] =
        vertexSqlCreates
          .map(vertexCreate => vertexCreate.createRule.reference -> createSchemaTable(vertexCreate))
          .toMap
      val edgeRefToDataMap: Map[Reference, Table[DataFrame]] =
        edgeSqlCreates
          .map(edgeCreate => edgeCreate.createRule.reference -> createSchemaTable(edgeCreate))
          .toMap

      val edgeFromToMap: Map[Reference, (Reference, Reference)] =
        edgeSqlCreates
          .map(edgeSqlCreate => {
            val edgeRef: Reference = edgeSqlCreate.createRule.reference
            val fromRef: Reference = edgeSqlCreate.createRule.connType match {
              case OutConn => edgeSqlCreate.createRule.leftReference
              case InConn => edgeSqlCreate.createRule.rightReference
            }
            val toRef: Reference = edgeSqlCreate.createRule.connType match {
              case OutConn => edgeSqlCreate.createRule.rightReference
              case InConn => edgeSqlCreate.createRule.leftReference
            }
            edgeRef -> (fromRef, toRef)
          })
          .toMap
      val edgeRestrictions: LabelRestrictionMap =
        SchemaMap(
          edgeFromToMap.map {
            case (edgeRef, (fromRef, toRef)) =>
              val edgeLabel: Label = edgeRefToDataMap(edgeRef).name
              val fromLabel: Label = vertexRefToDataMap(fromRef).name
              val toLabel: Label = vertexRefToDataMap(toRef).name
              edgeLabel -> (fromLabel, toLabel)
          })

      ConstructClauseData(
        vertexDataMap = vertexRefToDataMap,
        edgeDataMap = edgeRefToDataMap,
        edgeRestrictions = edgeRestrictions)
    }
  }

  private def createSchemaTable(edgeCreate: sql.EdgeCreate): Table[DataFrame] = {
    val edgeRef: Reference = edgeCreate.createRule.reference
    val edgeData: DataFrame = solveBtableOps(edgeCreate)
    val edgeTable: Table[DataFrame] = createSchemaTable(edgeRef, edgeData)
    edgeTable
  }

  private def createSchemaTable(vertexCreate: sql.VertexCreate): Table[DataFrame] = {
    val vertexRef: Reference = vertexCreate.createRule.reference
    val vertexData: DataFrame = solveBtableOps(vertexCreate)
    val vertexTable: Table[DataFrame] = createSchemaTable(vertexRef, vertexData)
    vertexTable
  }

  private def createSchemaTable(reference: Reference, data: DataFrame): Table[DataFrame] = {
    val labelColumnSelect: String = s"${reference.refName}$$${tableLabelColumn.columnName}"
    val labelColumn: String = data.select(labelColumnSelect).first.getString(0)
    val newDataColumnNames: Seq[String] =
      data.columns.map(columnName => columnName.split(s"${reference.refName}$$")(0))
    val dataColumnsRenamed: DataFrame = data.drop(labelColumn).toDF(newDataColumnNames: _*)
    Table[DataFrame](name = Label(labelColumn), data = dataColumnsRenamed)
  }
}
