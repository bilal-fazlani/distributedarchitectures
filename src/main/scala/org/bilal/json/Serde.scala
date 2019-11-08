package org.bilal.json

import io.bullet.borer.{Decoder, Encoder, Json}

object Serde extends Codecs {
  def encodeToString[T:Encoder](value:T): String = Json.encode(value).toUtf8String
  def encodeToBytes[T:Encoder](value:T): Array[Byte] = Json.encode(value).to[Array[Byte]].result
  def decode[T:Decoder](text: String):T = Json.decode(text.getBytes()).to[T].value
  def decode[T:Decoder](bytes: Array[Byte]):T = Json.decode(bytes).to[T].value
}
