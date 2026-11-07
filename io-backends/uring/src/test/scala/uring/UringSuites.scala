package gears.async.io
package uring

import gears.async.io.testkit.*
import gears.io.testkit.SimpleIOChecks

/** Each suite sees only the capabilities required by its contracts. */
class UringContractSuite extends OpsContract {
  val backend: Backend = UringBackend
}

class UringPipeSuite extends PipeContract {
  val backend: Pipes = UringBackend
}

class UringSimpleIOSuite extends SimpleIOChecks with PipeFixture {
  val backend: Pipes = UringBackend
}

/** Runs the same stream contracts over TCP, without exposing Pipes to the suite. */
class UringSocketStreamSuite extends StreamContract with IpStreamFixture {
  val backend: IpStreams = UringBackend
}

class UringIpStreamSuite extends IpStreamContract {
  val backend: IpStreams = UringBackend
}

class UringUnixStreamSuite extends UnixStreamContract {
  val backend: UnixStreams = UringBackend
}

class UringSimpleIOSocketSuite extends SimpleIOChecks with IpStreamFixture {
  val backend: IpStreams = UringBackend
}

class UringSimpleIOIpStreamSuite extends gears.io.testkit.SimpleIOIpStreamChecks {
  val backend: IpStreams = UringBackend
}

class UringIpDatagramSuite extends IpDatagramContract {
  val backend: IpDatagrams = UringBackend
}

class UringFileSuite extends FileContract {
  val backend: RegularFiles = UringBackend
}

class UringResolveSuite extends ResolveContract {
  val backend: Resolving = UringBackend
}

class UringBlockingSuite extends BlockingContract {
  val backend: Blocking = UringBackend
}

class UringSimpleIOResolveSuite extends gears.io.testkit.SimpleIOResolveChecks {
  val backend: Resolving = UringBackend
}
