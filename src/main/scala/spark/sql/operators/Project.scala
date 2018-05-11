package spark.sql.operators

import algebra.expressions.Reference
import algebra.operators.Column._
import algebra.target_api
import algebra.target_api.{BindingTableMetadata, TargetTreeNode}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import spark.sql.SqlQuery

case class Project(relation: TargetTreeNode, attributes: Seq[Reference])
  extends target_api.Project(relation, attributes) {

  override val bindingTable: BindingTableMetadata = {
    val relationBtable: SqlBindingTableMetadata =
      relation.bindingTable.asInstanceOf[SqlBindingTableMetadata]
    val relationSchemaMap: Map[Reference, StructType] = relationBtable.schemaMap

    val existingRefs: Seq[Reference] =
      attributes.filter(attr => relationSchemaMap.get(attr).isDefined)
    val existingColumnsSelect: Set[String] =
      existingRefs
        .flatMap(ref => relationSchemaMap(ref).fields)
        .map(field => s"`${field.name}`")
        .toSet

    val refsNotInSchema: Set[Reference] =
      attributes
        .filter(attr => relationSchemaMap.get(attr).isEmpty)
        .toSet
    val refsInSchemaWithoutId: Set[Reference] =
      existingRefs // in schema
        .filterNot(ref =>
          relationSchemaMap(ref)
            .fields
            .exists(_.name == s"${ref.refName}$$${idColumn.columnName}")) // no id column
        .toSet
    val newRefs: Set[Reference] = refsNotInSchema ++ refsInSchemaWithoutId
    // For each new variable, we add a column containing monotonically increasing id's. We cannot
    // simply add any constant here, because it will be coalesced into a single group by a GROUP BY
    // clause. The monotonic id is actually one id per partition and, for each row on that partition
    // it increases by one. Therefore, in the resulting table, we may see very skewed id's in the
    // new columns. However, they should be distinct from each other and thus have all the new rows
    // preserved after the GROUP BY.
    val newRefsSelect: Set[String] =
      newRefs.map(
        ref => s"MONOTONICALLY_INCREASING_ID() AS `${ref.refName}$$${idColumn.columnName}`")

    val columnsToSelect: Set[String] = existingColumnsSelect ++ newRefsSelect

    val sqlProject: String =
      s"""
      SELECT ${columnsToSelect.mkString(", ")} FROM (${relationBtable.btable.resQuery})"""

    val refsInSchemaWithIdRefSchema: Map[Reference, StructType] =
      existingRefs
        .filterNot(ref => refsInSchemaWithoutId.contains(ref))
        .map(ref => ref -> relationSchemaMap(ref))
        .toMap
    val refsInSchemaWithoutIdRefSchema: Map[Reference, StructType] =
      refsInSchemaWithoutId
        .map(ref => {
          val previousSchema: StructType = relationSchemaMap(ref)
          val newIdStructField: StructField =
            StructField(s"${ref.refName}$$${idColumn.columnName}", IntegerType)
          ref -> StructType(previousSchema.fields :+ newIdStructField)
        })
        .toMap
    val refsNotInSchemaRefSchema: Map[Reference, StructType] =
      refsNotInSchema
        .map(ref =>
          ref -> StructType(Array(
            StructField(s"${ref.refName}$$${idColumn.columnName}", IntegerType))))
        .toMap

    val projectionRefSchema: Map[Reference, StructType] =
      refsInSchemaWithIdRefSchema ++ refsInSchemaWithoutIdRefSchema ++ refsNotInSchemaRefSchema
    val projectionSchema: StructType =
      StructType(projectionRefSchema.values.flatMap(_.fields).toArray)

    SqlBindingTableMetadata(
      sparkSchemaMap = projectionRefSchema,
      sparkBtableSchema = projectionSchema,
      btableOps = SqlQuery(resQuery = sqlProject))
  }
}
