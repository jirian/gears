package gears.io.net

import gears.async.io.Blocking
import gears.async.io.IpDatagrams
import gears.async.io.IpStreams
import gears.async.io.Pipes
import gears.async.io.RegularFiles
import gears.async.io.Resolving
import gears.async.io.UnixStreams

type DefaultCapabilities = IpStreams & IpDatagrams & UnixStreams & Resolving & RegularFiles & Pipes & Blocking
