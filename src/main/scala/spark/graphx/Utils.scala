package spark.graphx

import algebra.operators.Column.{EDGE_SEQ_COL, FROM_ID_COL, ID_COL, TO_ID_COL}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import spark.graphx.ShortestPaths.{EdgeId, VertexInfoMap}

object Utils {
  private def COST_COL: String = "path_cost"
  private def SP_INFO_COL: String = "sp_info"

  /**
    * The attributes of vertices and edges are of type Row. => VD = Row, ED = Row
    */
  def createPathData(edgeData: DataFrame, fromData: DataFrame, toData: DataFrame,
                     sparkSession: SparkSession): DataFrame = {
    val vertexRDD: RDD[(VertexId, VertexId)] =
      fromData
        .select(col(ID_COL.columnName)).union(toData.select(col(ID_COL.columnName)))
        .select(col(ID_COL.columnName).cast("long"))
        .rdd
        .map { case Row(id: VertexId) => (id, id) }
    val edgeRDD: RDD[Edge[EdgeId]] =
      edgeData
        .select(
          col(ID_COL.columnName).cast("long"),
          col(FROM_ID_COL.columnName).cast("long"),
          col(TO_ID_COL.columnName).cast("long"))
        .rdd
        .map {
          case Row(edgeId: Long, fromId: Long, toId: Long) => Edge(fromId, toId, edgeId)
        }
    val graph: Graph[Long, EdgeId] = Graph(vertexRDD, edgeRDD)

    // TODO: Can we avoid collect here?
    val landmarks: Array[Long] =
      toData
        .select(col(ID_COL.columnName).cast("long"))
        .collect
        .map(_.getLong(0))

    val graphWithShortestPaths: Graph[VertexInfoMap, EdgeId] = ShortestPaths.run(graph, landmarks)

    sparkSession
      .createDataFrame(
        graphWithShortestPaths
          .mapVertices((_, vertexInfoMap) =>
            vertexInfoMap.toSeq.map {
              case (vertexId, (distance, edgeSeq)) => vertexId -> (distance, edgeSeq.reverse)
            })
          .vertices)
      .toDF(FROM_ID_COL.columnName, SP_INFO_COL)
      .select(col(FROM_ID_COL.columnName), explode(col(SP_INFO_COL)).as(SP_INFO_COL))
      .select(
        col(FROM_ID_COL.columnName),
        col(s"$SP_INFO_COL._1").as(TO_ID_COL.columnName),
        col(s"$SP_INFO_COL._2._1").as(COST_COL),
        col(s"$SP_INFO_COL._2._2").as(EDGE_SEQ_COL.columnName))
      .where(s"$COST_COL > 0")
  }
}
