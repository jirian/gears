package gears.io.net

import gears.async.io.IpAddress
import gears.async.io.Protocol

class IpLiteralSuite extends munit.FunSuite {

  private def v6(text: String) = IpLiteral.parse(text).map(_.toString)

  test("dotted IPv4") {
    assertEquals(IpLiteral.parse("127.0.0.1"), Some(IpAddress.IPv4Loopback))
    assertEquals(IpLiteral.parse("0.0.0.0"), Some(IpAddress.IPv4Any))
    assertEquals(IpLiteral.parse("192.168.1.255").map(_.toString), Some("192.168.1.255"))
  }

  test("IPv6, with and without a gap") {
    assertEquals(IpLiteral.parse("::1"), Some(IpAddress.IPv6Loopback))
    assertEquals(IpLiteral.parse("::"), Some(IpAddress.IPv6Any))
    assertEquals(v6("2001:db8::8a2e:370:7334"), Some("2001:db8::8a2e:370:7334"))
    assertEquals(v6("2001:db8:0:0:0:0:2:1"), Some("2001:db8::2:1"))
    assertEquals(v6("fe80::"), Some("fe80::"))
    assertEquals(v6("1:2:3:4:5:6:7:8"), Some("1:2:3:4:5:6:7:8"))
  }

  test("IPv6 with a dotted tail and a numeric scope") {
    assertEquals(v6("::ffff:192.0.2.1"), Some("::ffff:192.0.2.1"))
    assertEquals(v6("64:ff9b::192.0.2.1"), Some("64:ff9b::c000:201"))
    val scoped = IpLiteral.parse("fe80::1%3").get
    assertEquals((scoped.protocol, scoped.scopeId), (Protocol.IPv6, 3))
  }

  test("anything else is not a literal") {
    for text <- List(
        "localhost",
        "example.com",
        "",
        "1.2.3",
        "1.2.3.4.5",
        "256.0.0.1",
        "1.2.3.-4",
        ":::",
        "1::2::3",
        "1:2:3:4:5:6:7",
        "1:2:3:4:5:6:7:8:9",
        "1:2:3:4:5:6:7::8",
        "12345::",
        "1.2.3.4::",
        "::1.2.3.4:5",
        "fe80::1%",
        "fe80::1%eth0",
        "g::1"
      )
    do assertEquals(IpLiteral.parse(text), None, text)
  }
}
