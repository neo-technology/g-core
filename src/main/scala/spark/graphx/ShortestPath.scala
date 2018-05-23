package spark.graphx

import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.lib.ShortestPaths.SPMap
import org.apache.spark.graphx.{lib => graphxlib}
import org.apache.spark.sql.Row

object ShortestPath {

  def run(graph: Graph[Row, Row], landmarks: Seq[Long]): Graph[SPMap, Row] =
    graphxlib.ShortestPaths.run(graph, landmarks)
}
