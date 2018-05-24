package spark.graphx

import algebra.operators.Column.{FROM_ID_COL, ID_COL, TO_ID_COL}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.graphx.{lib => graphxlib}
import org.apache.spark.graphx.lib.ShortestPaths.SPMap
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, explode, struct}

object Utils {
  private def COST_COL: String = "path_cost"
  private def SP_DISTS_COL: String = "sp_distances"
  private def NESTED_ATTRS_COL: String = "nested_attrs"

  def createPathData(edgeData: DataFrame, fromData: DataFrame, toData: DataFrame,
                     sparkSession: SparkSession): DataFrame = {
    val graph: Graph[Row, Row] =
      convertToGraphX(
        fromData.select(col(ID_COL.columnName)).union(toData.select(col(ID_COL.columnName))),
        edgeData.select(
          col(ID_COL.columnName), col(FROM_ID_COL.columnName), col(TO_ID_COL.columnName)))

    // We need the set of landmarks as a sequence of longs.
    // TODO: Can we avoid collect here?
    val landmarks: Array[Long] =
    toData
      .select(col(ID_COL.columnName).cast("long"))
      .collect
      .map(_.getLong(0))

    // SPMap = Map[reachable VertexId, cost]
    val graphWithShortestPaths: Graph[SPMap, Row] = run(graph, landmarks)

    convertToPathTable(sparkSession, graphWithShortestPaths)
  }

  /**
    * The attributes of vertices and edges are of type Row. => VD = Row, ED = Row
    */
  private def convertToGraphX(vertexDf: DataFrame, edgeDf: DataFrame): Graph[Row, Row] = {
    val vertexRDD: RDD[(Long, Row)] =
      vertexDf
        .select(col(ID_COL.columnName).cast("long"), struct("*").as(NESTED_ATTRS_COL))
        .rdd
        .map { case Row(id: Long, attributes: Row) => (id, attributes) }
    val edgeRDD: RDD[Edge[Row]] =
      edgeDf
        .select(
          col(FROM_ID_COL.columnName).cast("long"),
          col(TO_ID_COL.columnName).cast("long"),
          struct("*").as(NESTED_ATTRS_COL))
        .rdd
        .map {
          case Row(fromId: Long, toId: Long, attributes: Row) => Edge(fromId, toId, attributes)
        }

    Graph(vertexRDD, edgeRDD)
  }

  private def convertToPathTable(sparkSession: SparkSession,
                                 graphShortestPaths: Graph[SPMap, Row]): DataFrame = {
    sparkSession
      .createDataFrame(graphShortestPaths.mapVertices((_, spMap) => spMap.toSeq).vertices)
      .toDF(FROM_ID_COL.columnName, SP_DISTS_COL)
      .select(col(FROM_ID_COL.columnName), explode(col(SP_DISTS_COL)).as(SP_DISTS_COL))
      .select(
        col(FROM_ID_COL.columnName),
        col(s"$SP_DISTS_COL._1").as(TO_ID_COL.columnName),
        col(s"$SP_DISTS_COL._2").as(COST_COL))
      .where(s"$COST_COL > 0")
  }

  private def run(graph: Graph[Row, Row], landmarks: Seq[Long]): Graph[SPMap, Row] =
    graphxlib.ShortestPaths.run(graph, landmarks)
}
