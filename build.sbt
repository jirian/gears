import org.scalajs.jsenv.nodejs.*
import org.scalajs.linker.interface.ESVersion
import sbtcrossproject.CrossPlugin.autoImport.{CrossType, crossProject}

scalaVersion := "3.9.0"

// The Scala Native nightly selected in project/plugins.sbt brings its libraries from the snapshot repository.
ThisBuild / resolvers ++= (if (sys.env.get("SCALA_NATIVE_NIGHTLY").contains("1")) Seq(Resolver.sonatypeCentralSnapshots)
                           else Nil)

LocalRootProject / publish / skip := true

val MUnitFramework = new TestFramework("munit.Framework")

// Common publishing settings for all platforms.
organization := "ch.epfl.lamp"
homepage := Some(uri("https://lampepfl.github.io/gears"))
licenses := List(License.Apache2)
developers := List(
  Developer("natsukagami", "Natsu Kagami", "nki@fastmail.com", uri("https://github.com/natsukagami"))
)

lazy val root =
  crossProject(JSPlatform, JVMPlatform, NativePlatform)
    .crossType(CrossType.Full)
    .in(file("."))
    .settings(
      Seq(
        name := "Gears",
        publish / skip := false,
        versionScheme := Some("early-semver"),
        libraryDependencies += "org.scalameta" %% "munit" % "1.3.6" % Test,
        testFrameworks += MUnitFramework
      )
    )
    .jvmSettings(
      Seq(
        scalacOptions += "-release:21"
      )
    )
    .nativeSettings(
      Seq(
        nativeConfig ~= { c =>
          c.withMultithreading(true)
        }
      )
    )
    .jsSettings(jsWasmSettings)

// Emit ES modules with the Wasm backend, run on Node.js 26+, which supports Wasm and JSPI, including the nested async
// stack fix.
lazy val jsWasmSettings = Seq(
  scalaJSLinkerConfig := {
    scalaJSLinkerConfig.value
      .withESFeatures(_.withESVersion(ESVersion.ES2022).withUseWebAssembly(true))
      .withWasmFeatures(_.withUseJSPI(true)) // enable js.async/js.await
      .withModuleKind(ModuleKind.ESModule) // required by the Wasm backend
  },
  jsEnv := Def.uncached {
    val config = NodeJSEnv
      .Config()
      .withArgs(
        List(
          "--stack-size=204800"
        )
      )
    new NodeJSEnv(config)
  }
)

// gears' I/O interface lives in the core project, in `gears.async.io` (shared, so every platform has it), with what a
// backend shares in `gears.async.io.internal` and the runner, `Io.run`, per platform. Backends live in projects of
// their own, each in a subpackage of `gears.async.io`, which is what lets them reach what the interface keeps
// `private[io]`.

val munit = "org.scalameta" %% "munit" % "1.3.6"

val mainargs = "com.lihaoyi" %% "mainargs" % "0.7.8"

// What every backend is checked against, published beside the core so that a backend written elsewhere can check
// itself against the same contracts: the contract suites (`gears.async.io.testkit`), `BufferArena`, buffers each
// platform allocates, the gears demos (`gears.io.demos`) and `SimpleIO`, with `SimpleIOChecks`, in `gears.io.testkit`.
// The demos and `SimpleIO` live outside `gears.async`, so they see only the public `gears.async.io` interface. Each
// takes the `Io` and `Async` it runs with, and a backend's demo application is what starts their runs. Built for every platform
// the core is, though only the JVM has a backend to run them on yet.
lazy val ioTestkit =
  crossProject(JSPlatform, JVMPlatform, NativePlatform)
    .crossType(CrossType.Full)
    .in(file("io-testkit"))
    .settings(
      name := "gears-io-testkit",
      publish / skip := false,
      versionScheme := Some("early-semver"),
      libraryDependencies ++= Seq(munit, mainargs),
      testFrameworks += MUnitFramework
    )
    .jvmSettings(scalacOptions += "-release:21")
    .nativeSettings(nativeConfig ~= { _.withMultithreading(true) })
    .jsSettings(jsWasmSettings)
    .dependsOn(root)

// The reference backend: the semantics of the interface on the JDK's channels, over a plain selector poll, with no
// regard for speed. It is what the contracts are checked against, not a backend to deploy.
lazy val ioReference =
  (project in file("io-reference"))
    .settings(
      publish / skip := true,
      scalacOptions += "-release:21",
      libraryDependencies += munit % Test,
      testFrameworks += MUnitFramework,
      Test / fork := true
    )
    .dependsOn(root.jvm, ioTestkit.jvm % Test)

// The reference backend's demo application: the testkit's gears demos over it, as `ioReferenceDemos/run <command>`.
lazy val ioReferenceDemos =
  (project in file("io-reference/demos"))
    .settings(
      publish / skip := true,
      scalacOptions += "-release:21",
      Compile / mainClass := Some("referencedemos.ReferenceDemos"),
      run / fork := true
    )
    .dependsOn(ioReference, ioTestkit.jvm)

// The runtime the native backends share (`gears.async.io.shareable`): a pool of workers, each with an inbox and a heap
// of timers, that is the program's scheduler and performs its ops, cancelling them safely. A backend gives each worker
// its poller by overriding `Poller.waitForDeadline` and `wake`. Scala Native only: on the JVM gears' fibers are
// virtual threads, which do not run on threads of ours. Its four files are Jamie Thompson's, from his branch
// `async-io-kqueue-basepoint` (44bb011): `Poller` and `TimerHeap` unchanged, `TimerHeapOp` and `WorkpoolScheduler`
// with small changes for completion-based backends (work already done is delivered, a time limit takes its op back
// first, atomic entry), to be offered back to him.
lazy val ioRuntime =
  (project in file("io-runtime"))
    .enablePlugins(ScalaNativePlugin)
    .settings(
      name := "gears-io-runtime",
      publish / skip := false,
      versionScheme := Some("early-semver"),
      libraryDependencies += munit % Test,
      testFrameworks += MUnitFramework,
      nativeConfig ~= { _.withMultithreading(true) }
    )
    .dependsOn(root.native, ioTestkit.native % Test)

// The io_uring backend (Linux 6.0 or later, Scala Native) on the shared runtime, through liburing, whose `static
// inline` helpers, and a synchronous cancel, are in a C shim among its native resources. Every program that links it
// needs liburing.
lazy val ioUring =
  (project in file("io-backends/uring"))
    .enablePlugins(ScalaNativePlugin)
    .settings(
      name := "gears-io-uring",
      publish / skip := false,
      versionScheme := Some("early-semver"),
      libraryDependencies += munit % Test,
      testFrameworks += MUnitFramework,
      nativeConfig ~= { _.withMultithreading(true) }
    )
    .dependsOn(ioRuntime % "compile->compile;test->test", ioTestkit.native % Test)

// The higher level for programs: byte streams over connections, TLS, and an HTTP/1.1 server and client with WebSockets
// (`gears.io.net`, `gears.io.tls`, `gears.io.http`), written against `gears.async.io` and run on each platform's
// default backend: the reference backend on the JVM, io_uring on Scala Native, which also needs OpenSSL for TLS.
lazy val ioHttp =
  crossProject(JVMPlatform, NativePlatform)
    .crossType(CrossType.Full)
    .in(file("io-http"))
    .settings(
      name := "gears-io-http",
      publish / skip := false,
      versionScheme := Some("early-semver"),
      libraryDependencies += munit % Test,
      testFrameworks += MUnitFramework
    )
    .jvmSettings(
      scalacOptions += "-release:21",
      Test / fork := true
    )
    .nativeSettings(nativeConfig ~= { _.withMultithreading(true) })
    .jvmConfigure(_.dependsOn(ioReference))
    .nativeConfigure(_.dependsOn(ioUring))

// io_uring's demo application: the testkit's gears demos over io_uring, as `ioUringDemos/run <command>`, and the gears
// bench (`GearsBench`, from bench/io-bench) over it, as `ioUringDemos/run bench`.
lazy val ioUringDemos =
  (project in file("io-backends/uring/demos"))
    .enablePlugins(ScalaNativePlugin)
    .settings(
      publish / skip := true,
      Compile / mainClass := Some("uringdemos.UringDemos"),
      Compile / unmanagedSourceDirectories += (ThisBuild / baseDirectory).value / "bench/io-bench/shared/src/main/scala",
      nativeConfig ~= { _.withMultithreading(true) }
    )
    .dependsOn(ioUring, ioTestkit.native)

// The HTTP bench's server: gears-io-http serving the routes its Go counterpart serves, for a load generator to measure
// against both (`ioHttpBenchNative/nativeLink`, `ioHttpBenchJVM/run <port>`).
lazy val ioHttpBench =
  crossProject(JVMPlatform, NativePlatform)
    .crossType(CrossType.Full)
    .in(file("bench/http-bench"))
    .settings(
      publish / skip := true,
      Compile / mainClass := Some("httpbench.Main")
    )
    .jvmSettings(scalacOptions += "-release:21", run / fork := true)
    .nativeSettings(nativeConfig ~= { _.withMultithreading(true) })
    .jvmConfigure(_.dependsOn(ioHttp.jvm))
    .nativeConfigure(_.dependsOn(ioHttp.native))
