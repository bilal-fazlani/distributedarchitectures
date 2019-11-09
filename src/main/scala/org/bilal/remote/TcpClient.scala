package org.bilal.remote

import java.io.{DataInputStream, DataOutputStream}
import java.net.Socket

import io.bullet.borer.Codec
import org.bilal.simplekafka2.codec.{Codecs, Serde}

import scala.util.Using

class TcpClient(socket: Socket) extends Codecs {
  socket.setSoTimeout(5000)

  def readAndHandleRequestThenSendResponse[Req: Codec](handler: Req => TcpResponse): Unit = {
    val bytes = read(socket)
    val message = Serde.decode[Req](bytes)
    val response = handler(message)
    write(socket, response.bytes)
  }

  def sendRequestAndThenReadResponse[Req: Codec, Res: Codec](requestMessage: Req): Res = {
    write(socket, Serde.encode(requestMessage))
    val responseBytes: Array[Byte] = read(socket)
    Serde.decode[Res](responseBytes)
  }

  private def write(socket: Socket, bytes: Array[Byte]): Unit = {
    val outputStream = socket.getOutputStream
    val dataStream = new DataOutputStream(outputStream)
    val messageBytes = bytes
    dataStream.writeInt(messageBytes.length)
    dataStream.write(messageBytes)
    outputStream.flush()
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
object TcpClient{
  def sendReceiveTcp[A:Codec,B:Codec](request: A, to: (String, Int)): B = {
    Using.resource(new Socket(to._1, to._2)) {
      new TcpClient(_)
      .sendRequestAndThenReadResponse[A, B](request)
    }
  }
}
