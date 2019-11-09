package org.bilal.gossip.codec

import io.bullet.borer.Codec
import io.bullet.borer.derivation.MapBasedCodecs.deriveCodec
import org.bilal.gossip.api.{Address, Gossip}
import org.bilal.gossip.api.Gossip.{GossipRequest, GossipResponse}

trait Codecs {
  implicit lazy val addressCodec: Codec[Address] = deriveCodec
  implicit lazy val reqCodec: Codec[GossipRequest] = deriveCodec
  implicit lazy val resCodec: Codec[GossipResponse] = deriveCodec
  implicit lazy val gosCodec: Codec[Gossip] = deriveCodec
}
