package common.trees

trait TestTreeWrapper {

  /**
    *        1
    *       / \
    *      2  3
    *     / \
    *    4  5
    */
  def tree: IntTree = IntTree(
    value = 1,
    descs = List(
      IntTree(
        value = 2,
        descs = List(
          IntTree(
            value = 4,
            descs = List.empty),
          IntTree(
            value = 5,
            descs = List.empty))
      ),
      IntTree(
        value = 3,
        descs = List.empty))
  )

  /**
    *         1
    *        /
    *       2
    *     / | \
    *    3  4  5
    */
  def multiChildTree: IntTree = IntTree(
    value = 1,
    descs = List(
      IntTree(
        value = 2,
        descs = List(
          IntTree(
            value = 3,
            descs = List.empty),
          IntTree(
            value = 4,
            descs = List.empty),
          IntTree(
            value = 5,
            descs = List.empty))
      )
    )
  )

  case class IntTree(value: Int, descs: Seq[IntTree]) extends TreeNode[IntTree] {
    children = descs
  }
}
