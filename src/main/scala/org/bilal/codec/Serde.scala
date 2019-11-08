package org.bilal.codec

import io.bullet.borer.{Cbor, Decoder, Encoder, Target}

object Serde extends Codecs {
  val protocol: Target = Cbor
  def encode[T:Encoder](value:T): Array[Byte] = protocol.encode(value).to[Array[Byte]].result
  def decode[T:Decoder](bytes: Array[Byte]):T = protocol.decode(bytes).to[T].value
}