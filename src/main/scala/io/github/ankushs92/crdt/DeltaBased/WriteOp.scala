package io.github.ankushs92.crdt.DeltaBased


class WriteOp
object WriteOp extends Enumeration {

  //Local write operation, or a merge request sent by another replica
  val LOCAL, REMOTE = Value

}
