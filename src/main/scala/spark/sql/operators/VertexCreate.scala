package spark.sql.operators

import algebra.operators.Column._
import algebra.operators.RemoveClause
import algebra.target_api
import algebra.target_api.{BindingTableMetadata, TargetTreeNode}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import spark.sql.SqlQuery

case class VertexCreate(relation: TargetTreeNode, createRule: target_api.VertexCreate)
  extends TargetTreeNode {

  private type SelectStr = String

  private val relationBtable: SqlBindingTableMetadata =
    relation.bindingTable.asInstanceOf[SqlBindingTableMetadata]
  private val reference: String = createRule.reference.refName
  private val idCol: String = s"$reference$$${idColumn.columnName}"
  private val constructIdCol: String = s"$reference$$${constructIdColumn.columnName}"
  private val labelCol: String = s"$reference$$${tableLabelColumn.columnName}"

  override val bindingTable: BindingTableMetadata = {
    val existingFields: Map[StructField, SelectStr] = createExistingFieldsMap
    val newFields: Map[StructField, SelectStr] = createNewFieldsMap
    val removeFields: Set[StructField] = createRemoveFieldsSet(existingFields.keySet)
    val fieldsToSelect: Map[StructField, SelectStr] = (existingFields -- removeFields) ++ newFields
    val columnsToSelect: String = fieldsToSelect.values.mkString(", ")

    val createQuery: String =
      s"""
      SELECT $columnsToSelect FROM (${relationBtable.btableOps.resQuery})"""

    val newRefSchema: StructType = StructType(fieldsToSelect.keys.toArray)

    SqlBindingTableMetadata(
      sparkSchemaMap = Map(createRule.reference -> newRefSchema),
      sparkBtableSchema = newRefSchema,
      btableOps = SqlQuery(resQuery = createQuery))
  }

  private def createExistingFieldsMap: Map[StructField, SelectStr] = {
    relationBtable.btableSchema.fields
      .map(field => field -> s"`${field.name}`")
      .toMap
  }

  private def createNewFieldsMap: Map[StructField, SelectStr] = {
    // We add the new id field, which is created based on the construct_id.
    val idField: StructField = StructField(idCol, IntegerType)
    val idSelect: SelectStr = s"(${createRule.tableBaseIndex} + `$constructIdCol` - 1) AS `$idCol`"

    // TODO: If the label is missing, add it as a new column and create a random name for the label.
    Map(idField -> idSelect)
  }

  private def createRemoveFieldsSet(existingFields: Set[StructField]): Set[StructField] = {
    val removeClauseColumns: Set[String] = {
      if (createRule.removeClause.isDefined) {
        val removeClause: RemoveClause = createRule.removeClause.get
        val labelRemoves: Set[String] = removeClause.labelRemoves.map(_ => s"$labelCol").toSet
        val propRemoves: Set[String] =
          removeClause.propRemoves
            .map(propRemove => s"$reference$$${propRemove.propertyRef.propKey.key}")
            .toSet
        labelRemoves ++ propRemoves
      } else {
        Set.empty
      }
    }
    existingFields.filter(field =>
      // Remove the old id column and the construct id column.
      field.name == idCol || field.name == constructIdCol ||
        // Remove all the properties and labels specified in the createRule.removeClause
        removeClauseColumns.contains(field.name)
    )
  }
}
