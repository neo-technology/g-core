package spark.sql.operators

import algebra.expressions.DisjunctLabels
import algebra.operators.Column.{idColumn, fromIdColumn, toIdColumn}
import algebra.operators.VirtualPathRelation
import algebra.target_api
import algebra.target_api.BindingTableMetadata
import algebra.types.{Graph, KleeneStar}
import common.exceptions.UnsupportedOperation
import org.apache.spark.graphx
import org.apache.spark.graphx.lib.ShortestPaths.SPMap
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row}
import schema.{Catalog, Table}
import spark.graphx.ShortestPath
import spark.graphx.Utils.convertToGraphX

case class PathSearch(pathRelation: VirtualPathRelation, graph: Graph, catalog: Catalog)
  extends target_api.PathSearch(pathRelation, graph, catalog) {

  override val bindingTable: BindingTableMetadata = {
    val fromData: DataFrame =
      physGraph.tableMap(fromTableName).asInstanceOf[Table[DataFrame]].data
    val toData: DataFrame =
      physGraph.tableMap(toTableName).asInstanceOf[Table[DataFrame]].data
    val edgeTable: DataFrame = pathRelation.pathExpression match {
      case Some(KleeneStar(DisjunctLabels(Seq(edgeTableName)), _, _)) =>
        physGraph.tableMap(edgeTableName).asInstanceOf[Table[DataFrame]].data
      case _ =>
        throw UnsupportedOperation("Unsupported path configuration.")
    }

    val graph: graphx.Graph[Row, Row] =
      convertToGraphX(
        vertexDf =
          fromData.select(col(idColumn.columnName)).union(toData.select(col(idColumn.columnName))),
        edgeDf =
          edgeTable.select(
            col(idColumn.columnName), col(fromIdColumn.columnName), col(toIdColumn.columnName)))

    // We need the set of landmarks as a sequence of longs.
    val landmarks: Array[Long] =
      toData
        .select(col(idColumn.columnName).cast("long"))
        .map(_.getLong(0))
        .collect

    val graphShortestPaths: graphx.Graph[SPMap, Row] = ShortestPath.run(graph, landmarks)
  }
}
