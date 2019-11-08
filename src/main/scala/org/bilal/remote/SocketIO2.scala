package org.bilal.remote

import java.io.{DataInputStream, DataOutputStream}
import java.net.Socket

import io.bullet.borer.Codec
import org.bilal.json.{Codecs, Serde}

import scala.util.Using

class SocketIO2[Req:Codec, Res:Codec](clientSocket: Socket) extends Codecs{
  clientSocket.setSoTimeout(5000)

  def readHandleWithSocket(handler:(Req, Socket) => Unit): Unit = {
    val responseBytes = read(clientSocket)
    val message = Serde.decode[Req](responseBytes)
    handler(message, clientSocket)
  }

  def readHandleRespond(handler:Req => Res): Unit = {
    Using.resource(clientSocket) { socket =>
      val bytes = read(socket)
      val message = Serde.decode[Req](bytes)
      val response = handler(message)
      write(socket, Serde.encode(response))
    }
  }

  def read[T:Codec](): T = {
    Using.resource(clientSocket) { socket =>
      val bytes = read(socket)
      Serde.decode[T](bytes)
    }
  }

  def write[T:Codec](value:T):Unit = {
    Using.resource(clientSocket) { socket =>
      write(socket, Serde.encode(value))
    }
  }

  private def read(socket: Socket): Array[Byte] = {
    val inputStream = socket.getInputStream
    val dataInputStream = new DataInputStream(inputStream)
    val size = dataInputStream.readInt()
    val responseBytes = new Array[Byte](size)
    dataInputStream.read(responseBytes)
    responseBytes
  }

  def requestResponse(requestMessage: Req): Res = {
    Using.resource(clientSocket) { socket =>
      write(socket, Serde.encode(requestMessage))
      val responseBytes: Array[Byte] = read(socket)
      Serde.decode[Res](responseBytes)
    }
  }

  private def write(socket: Socket, bytes: Array[Byte]): Unit = {
    val outputStream = socket.getOutputStream()
    val dataStream = new DataOutputStream(outputStream)
    val messageBytes = bytes
    dataStream.writeInt(messageBytes.length)
    dataStream.write(messageBytes)
    outputStream.flush()
  }
}
