package gears.io.reactor

import gears.io.unsafe.PosixSockets

import java.io.IOException
import java.nio.charset.StandardCharsets
import scala.scalanative.posix.arpa.inet
import scala.scalanative.posix.net
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

/** Conversion between `Address` and kernel `sockaddr` bytes. Encoded addresses live in heap arrays, which may be handed
  * to the kernel because Scala Native's GC doesn't move objects.
  */
object SocketAddresses {
  final val storageSize = sizeOf[socket.sockaddr_storage].toInt

  def storage(): Array[Byte] = new Array[Byte](storageSize)

  def pointer(storage: Array[Byte]): Ptr[socket.sockaddr] =
    storage.asInstanceOf[ByteArray].at(0).asInstanceOf[Ptr[socket.sockaddr]]

  /** Runs `use` with `address` encoded as a `sockaddr`. An IPv6 host may end in `%` and a scope: the number or name of
    * the network interface that a link-local address belongs to.
    *
    * @tparam A
    *   the result type
    * @param address
    *   the address to encode
    * @param use
    *   what to run with the encoded address and its length
    * @return
    *   the result of `use`
    */
  def withAddress[A](address: Address)(use: (Ptr[socket.sockaddr], socket.socklen_t) => A): A =
    address match {
      case Address.Unix(path)       => PosixSockets.withUnixSocketAddr(path)(use)
      case Address.IPv4(host, port) => PosixSockets.withIPv4SocketAddr(host, port)(use)
      case Address.IPv6(host, port) =>
        val percent = host.indexOf('%')
        if percent < 0 then PosixSockets.withIPv6SocketAddr(host, port)(use)
        else {
          val scope = scopeId(host.substring(percent + 1))
          PosixSockets.withIPv6SocketAddr(host.substring(0, percent), port) { (addr, length) =>
            addr.asInstanceOf[Ptr[in.sockaddr_in6]].sin6_scope_id = scope
            use(addr, length)
          }
        }
    }

  private def scopeId(scope: String): CUnsignedInt =
    if scope.nonEmpty && scope.forall(_.isDigit) then java.lang.Integer.parseUnsignedInt(scope).toUInt
    else {
      val index = Zone.acquire { implicit z => net.`if`.if_nametoindex(toCString(scope)) }
      if index == 0.toUInt then throw new IllegalArgumentException(s"Unknown network interface in IPv6 scope: $scope")
      index
    }

  def encode(address: Address, storage: Array[Byte]): socket.socklen_t =
    withAddress(address) { (addr, length) =>
      string.memcpy(pointer(storage).asInstanceOf[Ptr[Byte]], addr.asInstanceOf[Ptr[Byte]], length.toLong.toCSize)
      length
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
        val scope = v6.sin6_scope_id
        val host = if scope == 0.toUInt then fromCString(text) else s"${fromCString(text)}%${scope.toLong}"
        Address.IPv6(host, inet.ntohs(v6.sin6_port).toInt)
      } else throw new IOException(s"Unsupported address family $family")
    }
  }
}
