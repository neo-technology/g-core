package ir.spoofax

import ir.Node

/**
  * MatchClause.Match = <MATCH <FullGraphPatternCondition> <OptionalClause?>
  * FullGraphPatternCondition.FullGraphPatternCondition = <<FullGraphPattern> <WhereClause?>>
  * OptionalClause.OptionalClause = <<{Optional "\n"}+>>
  * Optional.Optional = <OPTIONAL <FullGraphPatternCondition>>
  *
  * MATCH over a sequence of FullGraphPatternCondition clauses. The first one is the pattern given
  * to the MATCH directly and, if there is more than one, the rest correspond to the patterns under
  * the OPTIONAL clauses.
  */
case class MatchClause(patterns: Seq[Node]) extends Node {
  name = "MatchClause"
  children = patterns
}

case class FullGraphPatternCondition(patterns: Node, // FullGraphPattern
                                     whereClause: Node,
                                     isOptional: Boolean = false) extends Node {
  name = "FullGraphPatternCondition"
  children = List(patterns, whereClause)

  override def print(level: Int): Unit = {
    val optional: String = if (isOptional) " [optional]" else ""
    println(" " * level + name + optional)
    children.foreach(child => child.print(level + 2))
  }
}

/**
  * FullGraphPatternCondition.FullGraphPatternCondition = <<FullGraphPattern> <WhereClause?>>
  * FullGraphPattern.FullGraphPattern = <<{BasicGraphPatternLocation ",\n"}+>>
  * WhereClause.Where = <WHERE <Exp>> {case-insensitive}
  *
  * We express FullGraphPatternCondition as:
  *   FullGraphPatternCondition = (
  *     FullGraphPattern = List(BasicGraphPatternLocation = Tuple(BasicGraphPattern, Location)),
  *     WhereClause
  *   )
  *
  * BasicGraphPattern.BasicGraphPattern = <<VertexMatchPattern> <EdgeVertexMatchPattern*>>
  */
case class FullGraphPattern(patterns: Seq[Node]) extends Node { // Seq[BasicGraphPatternLocation]
  name = "FullGraphPattern"
  children = patterns
}
case class BasicGraphPatternLocation(pattern: Node, location: Node) extends Node {
  name = "BasicGraphPatternLocation"
  children = List(pattern, location)
}

case class Expression() extends Node {
  name = "Expression"
}
case class Location() extends Node {
  name = "Location"
}
case class WhereClause() extends Node {
  name = "WhereClause"
}
