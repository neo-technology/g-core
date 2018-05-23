package spark.graphx

import algebra.operators.Column.{fromIdColumn, graphxAttrs, idColumn, toIdColumn}
import org.apache.spark.graphx.lib.ShortestPaths.SPMap
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, struct}

object Utils {

  /**
    * The attributes of vertices and edges are of type Row. => VD = Row, ED = Row
    */
  def convertToGraphX(vertexDf: DataFrame, edgeDf: DataFrame): Graph[Row, Row] = {
    val vertexRDD: RDD[(Long, Row)] =
      vertexDf
        .select(col(idColumn.columnName).cast("long"), struct("*").as(graphxAttrs.columnName))
        .rdd
        .map { case Row(id: Long, attributes: Row) => (id, attributes) }
    val edgeRDD: RDD[Edge[Row]] =
      edgeDf
        .select(
          col(fromIdColumn.columnName).cast("long"),
          col(toIdColumn.columnName).cast("long"),
          struct("*").as(graphxAttrs.columnName))
        .rdd
        .map {
          case Row(fromId: Long, toId: Long, attributes: Row) => Edge(fromId, toId, attributes)
        }

    Graph(vertexRDD, edgeRDD)
  }

  def shortestPathsToPathTable(graphShortestPaths: Graph[SPMap, Row],
                               fromData: DataFrame,
                               toData: DataFrame): DataFrame = {

  }
}
