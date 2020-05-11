package io.github.ankushs92.crdt

import io.github.ankushs92.crdt.AggType.{apply => _}

case class WindowPartialComputation(aggType : AggType.Value, value : Byte) {

}

