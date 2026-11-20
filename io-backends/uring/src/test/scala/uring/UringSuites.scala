package gears.async.io
package uring

import gears.async.io.testkit.*
import gears.io.testkit.SimpleIOChecks

class UringContractSuite extends OpsContract {
  val backend: Backend = UringBackend
}

class UringPipeSuite extends PipeContract {
  val backend: Backend { type Interface <: Pipes } = UringBackend
}

class UringSimpleIOSuite extends SimpleIOChecks with PipeFixture {
  val backend: Backend { type Interface <: Pipes } = UringBackend
}

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

class UringUnixDatagramSuite extends UnixDatagramContract {
  val backend: Backend { type Interface <: UnixDatagrams } = UringBackend
}

class UringFifoSuite extends FifoContract {
  val backend: Backend { type Interface <: Fifos } = UringBackend
}

class UringFifoFileSuite extends FifoFileContract {
  val backend: Backend { type Interface <: Fifos & RegularFiles } = UringBackend
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
