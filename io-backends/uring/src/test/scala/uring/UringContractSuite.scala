package gears.async.io
package uring

import gears.async.io.testkit.FifoContract
import gears.async.io.testkit.ResolveContract
import gears.async.io.testkit.SocketContract
import gears.async.io.testkit.UnixDatagramContract
import gears.async.io.testkit.UnixStreamContract

class UringContractSuite
    extends SocketContract
    with UnixStreamContract
    with UnixDatagramContract
    with FifoContract
    with ResolveContract {
  val backend: UringBackend.type = UringBackend
}
