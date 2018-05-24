package spark.graphx

import org.apache.spark.graphx.{EdgeTriplet, Graph, Pregel, VertexId}

object ShortestPaths {

  type Distance = Int
  type EdgeId = Long
  type VertexInfoMap = Map[VertexId, (Distance, Seq[EdgeId])]

  def run(graph: Graph[VertexId, EdgeId], landmarks: Seq[Long]): Graph[VertexInfoMap, EdgeId] = {
    val processedGraph: Graph[VertexInfoMap, EdgeId] =
      graph.mapVertices {
        case (vertexId, _) =>
          if (landmarks.contains(vertexId)) Map(vertexId -> (0, Seq.empty))
          else Map.empty
      }

    Pregel(
      graph = processedGraph,
      initialMsg = Map.empty[VertexId, (Distance, Seq[EdgeId])])(
      vprog = vertexProgram,
      sendMsg = sendMessage,
      mergeMsg = mergeMaps)
  }

  private def vertexProgram(vertexId: VertexId,
                            vertexInfoMap: VertexInfoMap,
                            msgInfoMap: VertexInfoMap): VertexInfoMap =
    mergeMaps(vertexInfoMap, msgInfoMap)

  private def mergeMaps(map1: VertexInfoMap, map2: VertexInfoMap): VertexInfoMap = {
    (map1.keySet ++ map2.keySet).map {
      vertexId =>
        val dist1: Distance =
          if (map1.contains(vertexId)) map1(vertexId)._1
          else Int.MaxValue
        val dist2: Distance =
          if (map2.contains(vertexId)) map2(vertexId)._1
          else Int.MaxValue

        if (dist1 < dist2) vertexId -> map1(vertexId)
        else vertexId -> map2(vertexId)
    }.toMap
  }

  private def incrementMap(edgeId: EdgeId, dstInfoMap: VertexInfoMap): VertexInfoMap = {
    dstInfoMap.map {
      case (vertexId, (distance, edgeSeq)) => vertexId -> (distance + 1, edgeSeq :+ edgeId)
    }
  }

  private def sendMessage(edge: EdgeTriplet[VertexInfoMap, EdgeId])
  : Iterator[(VertexId, VertexInfoMap)] = {

    val newAttr: VertexInfoMap = incrementMap(edge.attr, edge.dstAttr)
    if (edge.srcAttr != mergeMaps(newAttr, edge.srcAttr))
      Iterator((edge.srcId, newAttr))
    else
      Iterator.empty
  }
}
