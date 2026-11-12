package gears.io.net

import gears.async.io.Blocking
import gears.async.io.IpDatagrams
import gears.async.io.IpStreams
import gears.async.io.Pipes
import gears.async.io.RegularFiles
import gears.async.io.Resolving
import gears.async.io.UnixStreams

/** What every platform's default backend can do (`Defaults.backend`, in gears-io-http-defaults): what a program that
  * runs on any of them may use.
  */
type DefaultCapabilities = IpStreams & IpDatagrams & UnixStreams & Resolving & RegularFiles & Pipes & Blocking
