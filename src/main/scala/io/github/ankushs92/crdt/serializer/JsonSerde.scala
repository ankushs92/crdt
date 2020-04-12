package io.github.ankushs92.crdt.serializer

import io.ankushs92.util.Json

class JsonSerde[IN: Manifest] extends Serde[IN, String] {

  override def serialize(in: IN) = Json.encode(in)

  override def deserialize(jsonStr: String): IN = Json.toObject[IN](jsonStr)

}
