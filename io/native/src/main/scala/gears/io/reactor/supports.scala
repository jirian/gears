package gears.io.reactor

import gears.io.file.FileSupport
import gears.io.net.{DnsSupport, TcpSupport, UdpSupport}

/** The network, file and DNS supports on any driver. */
extension (driver: ReactorDriver)

  /** Returns TCP and Unix stream sockets on this driver. */
  def tcpSupport: TcpSupport = new ReactorTcpSupport(driver) {}

  /** Returns UDP and Unix datagram sockets on this driver. */
  def udpSupport: UdpSupport = new ReactorUdpSupport(driver) {}

  /** Returns file I/O on this driver. */
  def fileSupport: FileSupport = new ReactorFileSupport(driver) {}

  /** Returns host name resolution on this driver. */
  def dnsSupport: DnsSupport = new ReactorDnsSupport(driver) {}
