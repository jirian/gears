package asyncio.uring

import asyncio.FifoContract
import asyncio.ReadinessContract
import asyncio.ResolveContract
import asyncio.UnixDatagramContract
import asyncio.UnixStreamContract

class UringContractSuite
    extends ReadinessContract
    with UnixStreamContract
    with UnixDatagramContract
    with FifoContract
    with ResolveContract {
  val backend: UringBackend.type = UringBackend
}
