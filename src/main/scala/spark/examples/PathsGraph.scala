package spark.examples

import algebra.expressions.Label
import org.apache.spark.sql.{DataFrame, SparkSession}
import schema.EntitySchema.LabelRestrictionMap
import schema.{SchemaMap, Table}
import spark.SparkGraph

case class PathsGraph(spark: SparkSession) extends SparkGraph {

  val A = Vertex(0, "A")
  val B = Vertex(1, "B")
  val C = Vertex(2, "C")
  val D = Vertex(3, "D")
  val E = Vertex(4, "E")
  val F = Vertex(5, "F")
  val G = Vertex(6, "G")

  val AB = Edge(10, 0, 1)
  val AE = Edge(11, 0, 4)
  val BC = Edge(12, 1, 2)
  val BE = Edge(13, 1, 4)
  val CD = Edge(14, 2, 3)
  val DE = Edge(15, 3, 4)
  val DF = Edge(16, 3, 5)
  val ED = Edge(17, 4, 3)
  val EG = Edge(18, 4, 6)
  val GA = Edge(19, 6, 0)

  import spark.implicits._

  override def graphName: String = "paths_graph"

  override def vertexData: Seq[Table[DataFrame]] = Seq(
    Table(
      name = Label("Vertex"),
      data = Seq(A, B, C, D, E, F, G).toDF)
  )

  override def edgeData: Seq[Table[DataFrame]] = Seq(
    Table(
      name = Label("Edge"),
      data = Seq(AB, AE, BC, BE, CD, DE, DF, ED, EG, GA).toDF)
  )

  override def pathData: Seq[Table[DataFrame]] = Seq.empty

  override def edgeRestrictions: LabelRestrictionMap = SchemaMap(Map(
    Label("Edge") -> (Label("Vertex"), Label("Vertex"))
  ))

  override def storedPathRestrictions: LabelRestrictionMap = SchemaMap.empty
}

sealed case class Vertex(id: Int, name: String)
sealed case class Edge(id: Int, fromId: Int, toId: Int)
