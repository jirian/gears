package gears.io.reactor

import gears.io.file.FileSupport
import gears.io.net.{DnsSupport, TcpSupport, UdpSupport}

extension (driver: ReactorDriver)
  def tcpSupport: TcpSupport = new ReactorTcpSupport(driver) {}
  def udpSupport: UdpSupport = new ReactorUdpSupport(driver) {}
  def fileSupport: FileSupport = new ReactorFileSupport(driver) {}
  def dnsSupport: DnsSupport = new ReactorDnsSupport(driver) {}
