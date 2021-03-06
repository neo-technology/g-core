package schema

import algebra.expressions.{Label, PropertyKey}
import algebra.types.{GcoreInteger, GcoreString}
import org.scalatest.FunSuite

class EntitySchemaTest extends FunSuite {

  val schema1 = EntitySchema(SchemaMap(Map(
    Label("person") -> SchemaMap(Map(
      PropertyKey("age") -> GcoreInteger,
      PropertyKey("address") -> GcoreString)),
    Label("city") -> SchemaMap(Map(
      PropertyKey("population") -> GcoreInteger))
  )))

  val schema2 = EntitySchema(SchemaMap(Map(
    Label("car") -> SchemaMap(Map(
      PropertyKey("type") -> GcoreString,
      PropertyKey("manufacturer") -> GcoreString))
  )))

  val schema1UnionSchema2 = EntitySchema(SchemaMap(Map(
    Label("person") -> SchemaMap(Map(
      PropertyKey("age") -> GcoreInteger,
      PropertyKey("address") -> GcoreString)),
    Label("city") -> SchemaMap(Map(
      PropertyKey("population") -> GcoreInteger)),
    Label("car") -> SchemaMap(Map(
      PropertyKey("type") -> GcoreString,
      PropertyKey("manufacturer") -> GcoreString))
  )))

  val emptySchema = EntitySchema.empty


  test("Union with other schema creates a schema with key-value pairs from both sides of union") {
    assert((schema1 union schema2) == schema1UnionSchema2)
  }

  test("Union with empty schema is idempotent") {
    assert((schema1 union emptySchema) == schema1)
  }

  test("Union is commutative") {
    assert((schema1 union schema2) == schema1UnionSchema2)
    assert((schema2 union schema1) == schema1UnionSchema2)
  }

  test("labels") {
    assert(schema1.labels == Seq(Label("person"), Label("city")))
  }

  test("properties(label)") {
    assert(schema1.properties(Label("person")) == Seq(PropertyKey("age"), PropertyKey("address")))
    assert(schema1.properties(Label("car")) == Seq.empty)
  }

  test("properties") {
    assert(
      schema1.properties ==
        Seq(PropertyKey("age"), PropertyKey("address"), PropertyKey("population")))
  }
}
