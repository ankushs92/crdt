package io.github.ankushs92.crdt.serializer

import io.github.ankushs92.crdt.util.Json


class JsonSerde[IN: Manifest] extends Serde[IN, String] {

  override def serialize(in: IN) = Json.encode(in)

  override def deserialize(jsonStr: String): IN = Json.toObject[IN](jsonStr)

}
