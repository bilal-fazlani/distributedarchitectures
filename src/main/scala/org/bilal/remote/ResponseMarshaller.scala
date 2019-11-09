package org.bilal.remote

import io.bullet.borer.Codec
import org.bilal.simplekafka2.codec.Serde

import scala.language.implicitConversions

trait ResponseMarshaller {
  protected implicit def convert[T:Codec](value:T):TcpResponse= TcpResponse(Serde.encode(value))
}
