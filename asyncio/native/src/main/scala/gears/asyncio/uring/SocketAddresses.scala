package gears.asyncio.uring

import gears.asyncio.Address
import gears.asyncio.unsafe.PosixSockets

import java.io.IOException
import java.nio.charset.StandardCharsets
import scala.scalanative.posix.arpa.inet
import scala.scalanative.posix.netinet.in
import scala.scalanative.posix.netinet.inOps.*
import scala.scalanative.posix.string
import scala.scalanative.posix.sys.socket
import scala.scalanative.posix.sys.socketOps.*
import scala.scalanative.posix.sys.un
import scala.scalanative.posix.sys.unOps.*
import scala.scalanative.runtime.ByteArray
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*

object SocketAddresses {
  final val storageSize = sizeOf[socket.sockaddr_storage].toInt

  def storage(): Array[Byte] = new Array[Byte](storageSize)

  def pointer(storage: Array[Byte]): Ptr[socket.sockaddr] =
    storage.asInstanceOf[ByteArray].at(0).asInstanceOf[Ptr[socket.sockaddr]]

  def encode(address: Address, storage: Array[Byte]): socket.socklen_t = {
    val copy: (Ptr[socket.sockaddr], socket.socklen_t) => socket.socklen_t = { (addr, length) =>
      string.memcpy(pointer(storage).asInstanceOf[Ptr[Byte]], addr.asInstanceOf[Ptr[Byte]], length.toLong.toCSize)
      length
    }
    address match {
      case Address.Unix(path)       => PosixSockets.withUnixSocketAddr(path)(copy)
      case Address.IPv4(host, port) => PosixSockets.withIPv4SocketAddr(host, port)(copy)
      case Address.IPv6(host, port) => PosixSockets.withIPv6SocketAddr(host, port)(copy)
    }
  }

  def decode(addr: Ptr[socket.sockaddr], length: socket.socklen_t): Address = {
    require(length.toLong >= sizeOf[socket.sa_family_t].toLong, s"Short socket address: ${length.toInt} bytes")
    require(length.toLong <= storageSize, s"Oversized socket address: ${length.toInt} bytes")
    val family = addr.sa_family.toInt
    if family == socket.AF_UNIX then {
      val unix = addr.asInstanceOf[Ptr[un.sockaddr_un]]
      val headerLength = sizeOf[un.sockaddr_un] - sizeOf[CArray[CChar, un._108]]
      if length.toInt <= headerLength.toInt then Address.Unix("")
      else Address.Unix(fromCString(unix.sun_path.at(0), StandardCharsets.UTF_8))
    } else {
      val text = stackalloc[Byte](64)
      if family == socket.AF_INET then {
        require(length.toLong >= sizeOf[in.sockaddr_in].toLong, s"Short IPv4 socket address: ${length.toInt} bytes")
        val v4 = addr.asInstanceOf[Ptr[in.sockaddr_in]]
        inet.inet_ntop(family, v4.at3.asInstanceOf[Ptr[Byte]], text, 64.toUInt)
        Address.IPv4(fromCString(text), inet.ntohs(v4.sin_port).toInt)
      } else if family == socket.AF_INET6 then {
        require(length.toLong >= sizeOf[in.sockaddr_in6].toLong, s"Short IPv6 socket address: ${length.toInt} bytes")
        val v6 = addr.asInstanceOf[Ptr[in.sockaddr_in6]]
        inet.inet_ntop(family, v6.at4.asInstanceOf[Ptr[Byte]], text, 64.toUInt)
        Address.IPv6(fromCString(text), inet.ntohs(v6.sin6_port).toInt)
      } else throw new IOException(s"Unsupported address family $family")
    }
  }
}
