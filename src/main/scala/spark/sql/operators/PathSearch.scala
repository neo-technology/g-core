package spark.sql.operators

import algebra.operators.VirtualPathRelation
import algebra.target_api
import algebra.target_api.BindingTableMetadata
import algebra.types.Graph
import schema.Catalog

case class PathSearch(pathRelation: VirtualPathRelation, graph: Graph, catalog: Catalog)
  extends target_api.PathSearch(pathRelation, graph, catalog) {

  override val bindingTable: BindingTableMetadata = {

  }
}
