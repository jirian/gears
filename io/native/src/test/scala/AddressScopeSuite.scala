package gears.io.reactor

import java.net.{Inet6Address, InetAddress, InetSocketAddress}

class AddressScopeSuite extends munit.FunSuite:
  private def roundTrip(address: Address): Address =
    val storage = SocketAddresses.storage()
    val length = SocketAddresses.encode(address, storage)
    SocketAddresses.decode(SocketAddresses.pointer(storage), length)

  test("an IPv6 scope id survives encoding and decoding"):
    assertEquals(roundTrip(Address.IPv6("fe80::1%1", 80)), Address.IPv6("fe80::1%1", 80))
    assertEquals(roundTrip(Address.IPv6("::1", 80)), Address.IPv6("::1", 80))

  test("an interface name works as an IPv6 scope"):
    val Address.IPv6(host, _) = roundTrip(Address.IPv6("fe80::1%lo", 80)): @unchecked
    assert(host.matches("fe80::1%[1-9][0-9]*"), host)
    intercept[IllegalArgumentException](roundTrip(Address.IPv6("fe80::1%no-such-interface", 80)))

  test("an InetSocketAddress keeps its IPv6 scope id both ways"):
    val bytes = InetAddress.getByName("fe80::1").getAddress
    val scoped = InetSocketAddress(Inet6Address.getByAddress(null, bytes, 1), 80)
    val Address.IPv6(host, port) = toAddress(scoped): @unchecked
    assert(host.endsWith("%1"), host)
    assertEquals(port, 80)
    assertEquals(fromAddress(toAddress(scoped)), scoped)
