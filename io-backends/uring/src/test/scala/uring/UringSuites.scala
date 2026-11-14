package gears.async.io
package uring

import gears.async.io.testkit.*
import gears.io.testkit.SimpleIOChecks

/** Each suite sees only the capabilities required by its contracts. */
class UringContractSuite extends OpsContract {
  val backend: Backend = UringBackend
}

class UringPipeSuite extends PipeContract {
  val backend: Backend { type Interface <: Pipes } = UringBackend
}

class UringSimpleIOSuite extends SimpleIOChecks with PipeFixture {
  val backend: Backend { type Interface <: Pipes } = UringBackend
}

/** Runs the same stream contracts over TCP, without exposing Pipes to the suite. */
class UringSocketStreamSuite extends StreamContract with IpStreamFixture {
  val backend: Backend { type Interface <: IpStreams } = UringBackend
}

class UringIpStreamSuite extends IpStreamContract {
  val backend: Backend { type Interface <: IpStreams } = UringBackend
}

class UringUnixStreamSuite extends UnixStreamContract {
  val backend: Backend { type Interface <: UnixStreams } = UringBackend
}

class UringSimpleIOSocketSuite extends SimpleIOChecks with IpStreamFixture {
  val backend: Backend { type Interface <: IpStreams } = UringBackend
}

class UringSimpleIOIpStreamSuite extends gears.io.testkit.SimpleIOIpStreamChecks {
  val backend: Backend { type Interface <: IpStreams } = UringBackend
}

class UringIpDatagramSuite extends IpDatagramContract {
  val backend: Backend { type Interface <: IpDatagrams } = UringBackend
}

class UringFileSuite extends FileContract {
  val backend: Backend { type Interface <: RegularFiles } = UringBackend
}

class UringResolveSuite extends ResolveContract {
  val backend: Backend { type Interface <: Resolving } = UringBackend
}

class UringBlockingSuite extends BlockingContract {
  val backend: Backend { type Interface <: Blocking } = UringBackend
}

class UringSimpleIOResolveSuite extends gears.io.testkit.SimpleIOResolveChecks {
  val backend: Backend { type Interface <: Resolving } = UringBackend
}
