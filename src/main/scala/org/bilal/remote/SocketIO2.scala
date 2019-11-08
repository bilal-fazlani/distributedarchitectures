package org.bilal.remote

import java.io.{DataInputStream, DataOutputStream}
import java.net.Socket

import io.bullet.borer.Codec
import org.bilal.codec.{Codecs, Serde}

class SocketIO2[Req: Codec, Res: Codec](socket: Socket) extends Codecs {
  socket.setSoTimeout(5000)

  def readHandleWithSocket(handler: (Req, Socket) => Unit): Unit = {
    val responseBytes = read(socket)
    val message = Serde.decode[Req](responseBytes)
    handler(message, socket)
  }

  def readHandleRespond(handler: Req => Res): Unit = {
    val bytes = read(socket)
    val message = Serde.decode[Req](bytes)
    val response = handler(message)
    write(socket, Serde.encode(response))
  }

  def read[T: Codec](): T = {
    val bytes = read(socket)
    Serde.decode[T](bytes)
  }

  def write[T: Codec](value: T): Unit = {
    write(socket, Serde.encode(value))
  }

  private def write(socket: Socket, bytes: Array[Byte]): Unit = {
    val outputStream = socket.getOutputStream
    val dataStream = new DataOutputStream(outputStream)
    val messageBytes = bytes
    dataStream.writeInt(messageBytes.length)
    dataStream.write(messageBytes)
    outputStream.flush()
  }

  def requestResponse(requestMessage: Req): Res = {
    write(socket, Serde.encode(requestMessage))
    val responseBytes: Array[Byte] = read(socket)
    Serde.decode[Res](responseBytes)
  }

  private def read(socket: Socket): Array[Byte] = {
    val inputStream = socket.getInputStream
    val dataInputStream = new DataInputStream(inputStream)
    val size = dataInputStream.readInt()
    val responseBytes = new Array[Byte](size)
    dataInputStream.read(responseBytes)
    responseBytes
  }
}
