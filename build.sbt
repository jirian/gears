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
    .jsSettings(
      Seq(
        // Emit ES modules with the Wasm backend
        scalaJSLinkerConfig := {
          scalaJSLinkerConfig.value
            .withESFeatures(_.withESVersion(ESVersion.ES2022).withUseWebAssembly(true))
            .withWasmFeatures(_.withUseJSPI(true)) // enable js.async/js.await
            .withModuleKind(ModuleKind.ESModule) // required by the Wasm backend
        },
        // Node.js 26+ supports Wasm and JSPI, including the nested async stack fix.
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
    )

// Buffers for tests and demos written against `asyncio` alone, allocated from an arena each platform provides: a Zone
// on Scala Native, foreign memory on the JVM. Kept out of the reactor interface on purpose.
lazy val asyncioTestkit =
  crossProject(JVMPlatform, NativePlatform)
    .crossType(CrossType.Full)
    .in(file("asyncio-testkit"))
    .settings(publish / skip := true)

// The interface, and nothing else: submitters and reactors, ops, handles and slots, backends and their kinds and
// capabilities, and what a program is given to perform with (`Io`). It knows the gears library and no runtime of
// its own: the two modules below cannot be referenced from here, by construction.
lazy val asyncio =
  crossProject(JVMPlatform, NativePlatform)
    .crossType(CrossType.Pure)
    .in(file("asyncio"))
    .settings(
      publish / skip := true,
      libraryDependencies += "org.scalameta" %% "munit" % "1.3.6" % Test,
      testFrameworks += MUnitFramework
    )
    // Its tests take tasks across threads.
    .nativeSettings(nativeConfig ~= { _.withMultithreading(true) })
    .dependsOn(root, asyncioTestkit % Test)

// The higher level the interface is for, over it alone: result-returning methods and a stream of chunks, outside
// the `asyncio` package, so that it sees only what a program sees. Its checks join every backend's contract suite.
lazy val higherLevel =
  crossProject(JVMPlatform, NativePlatform)
    .crossType(CrossType.Pure)
    .in(file("higherlevel"))
    .settings(
      publish / skip := true,
      libraryDependencies += "org.scalameta" %% "munit" % "1.3.6" % Test,
      testFrameworks += MUnitFramework
    )
    .nativeSettings(nativeConfig ~= { _.withMultithreading(true) })
    .dependsOn(asyncio % "compile->compile;test->test")

// Implementation the backends share, below the interface: the blocker pool and the timer heap.
lazy val asyncioUnsafe =
  crossProject(JVMPlatform, NativePlatform)
    .crossType(CrossType.Pure)
    .in(file("asyncio-unsafe"))
    .settings(publish / skip := true)
    .nativeSettings(nativeConfig ~= { _.withMultithreading(true) })
    .dependsOn(asyncio)

// The gears runtime over reactor backends: the pool of workers driving reactors, its scheduler, and the `Pool` an
// `OnPool` performs through. A platform with suspension support starts it (`ReactorFutures`).
lazy val asyncioGears =
  crossProject(JVMPlatform, NativePlatform)
    .crossType(CrossType.Pure)
    .in(file("asyncio-gears"))
    .settings(
      publish / skip := true,
      libraryDependencies += "org.scalameta" %% "munit" % "1.3.6" % Test,
      testFrameworks += MUnitFramework
    )
    .nativeSettings(nativeConfig ~= { _.withMultithreading(true) })
    .dependsOn(asyncio)

// ported from https://github.com/bishabosha/scala-native-async-io/commit/6e8320d36c71f75c9daf85065f5c89729f0825c0
lazy val nativeAsyncioLoop =
  (project in file("asyncio-native/loop"))
    .enablePlugins(ScalaNativePlugin)
    .settings(
      publish / skip := true,
      libraryDependencies += "org.scalameta" %% "munit" % "1.3.6" % Test,
      testFrameworks += MUnitFramework,
      // Its tests run reactors on several threads.
      nativeConfig ~= { _.withMultithreading(true) }
    )
    .dependsOn(asyncio.native % "compile->compile;test->test", asyncioUnsafe.native)

// The JVM's reactor backend: NIO selectors over the JDK's selectable channels, and threads for what the JDK can only
// block on.
lazy val jvmAsyncioNio =
  (project in file("asyncio-jvm/nio"))
    .settings(
      publish / skip := true,
      libraryDependencies += "org.scalameta" %% "munit" % "1.3.6" % Test,
      testFrameworks += MUnitFramework
    )
    .dependsOn(asyncio.jvm % "compile->compile;test->test", asyncioUnsafe.jvm)

// Gears on the JVM over reactors: `ReactorFutures.run` with gears' virtual threads beside reactor threads, or, opted
// into, virtual threads carried by the reactor pool (see asyncio-jvm/nio/src/main/scala/asyncio/nio/TODO.md).
lazy val jvmAsyncioGears =
  (project in file("asyncio-jvm/gears"))
    .settings(
      publish / skip := true,
      libraryDependencies += "org.scalameta" %% "munit" % "1.3.6" % Test,
      testFrameworks += MUnitFramework,
      Test / fork := true,
      // `Carriers.Reactors` gives virtual threads the pool as their scheduler, which the JDK keeps to itself, and pins
      // them to their worker while an op is submitted (see `VirtualCarriers`).
      Test / javaOptions ++= Seq(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/jdk.internal.vm=ALL-UNNAMED"
      )
    )
    .dependsOn(
      asyncio.jvm % "compile->compile;test->test",
      asyncioGears.jvm,
      jvmAsyncioNio % Test,
      asyncioTestkit.jvm % Test,
      higherLevel.jvm % "test->compile;test->test"
    )

// Gears on Scala Native over reactors: starts the pool of `asyncio`'s scheduler with Native's suspension (see
// asyncio/kqueue/TODO.md, "A thread-pooled scheduler"). The runner is written against the interface and the runtime
// alone; its tests run it over the kqueue backend.
lazy val nativeAsyncioGears =
  (project in file("asyncio-native/gears"))
    .enablePlugins(ScalaNativePlugin)
    .settings(
      publish / skip := true,
      libraryDependencies += "org.scalameta" %% "munit" % "1.3.6" % Test,
      testFrameworks += MUnitFramework,
      nativeConfig ~= { _.withMultithreading(true) }
    )
    .dependsOn(
      asyncioGears.native,
      nativeAsyncioLoop % "test->compile;test->test",
      higherLevel.native % "test->compile;test->test"
    )

// The NWFramework backend: reactors over Apple's Network.framework for IP sockets and Dispatch for pipes and FIFOs, with
// their C side in its resources (see asyncio-native/nwframework/TODO.md). macOS only; every executable that links it needs
// Network.framework. It shares the Native implementation kept in the loop module (`asyncio.unsafe`); its tests run it
// on the runner.
val networkFramework: Seq[String] =
  if (sys.props.getOrElse("os.name", "").startsWith("Mac")) Seq("-framework", "Network") else Nil
lazy val nativeAsyncioNWFramework =
  (project in file("asyncio-native/nwframework"))
    .enablePlugins(ScalaNativePlugin)
    .settings(
      publish / skip := true,
      libraryDependencies += "org.scalameta" %% "munit" % "1.3.6" % Test,
      testFrameworks += MUnitFramework,
      nativeConfig ~= { _.withMultithreading(true).withLinkingOptions(_ ++ networkFramework) }
    )
    .dependsOn(
      nativeAsyncioLoop % "compile->compile;test->test",
      nativeAsyncioGears % "test->compile;test->test",
      higherLevel.native % "test->compile;test->test"
    )

// The io_uring backend: reactors over Linux's io_uring, through liburing, with its `static inline` helpers in a C shim
// beside its resources. Linux only (5.11 or later); every executable that links it needs liburing. It shares the
// Native implementation kept in the loop module (`asyncio.unsafe`); its tests run it on the runner.
lazy val nativeAsyncioUring =
  (project in file("asyncio-native/uring"))
    .enablePlugins(ScalaNativePlugin)
    .settings(
      publish / skip := true,
      libraryDependencies += "org.scalameta" %% "munit" % "1.3.6" % Test,
      testFrameworks += MUnitFramework,
      nativeConfig ~= { _.withMultithreading(true) }
    )
    // Not the loop's or the runner's tests, which link kqueue.
    .dependsOn(
      nativeAsyncioLoop,
      asyncio.native % "test->test",
      nativeAsyncioGears % "test->compile",
      higherLevel.native % "test->compile;test->test"
    )

// The gears demos, written against `asyncio` alone, so the same code runs on kqueue (Scala Native, through
// kqueueDemo's `kqueuedemo gears ...`) and on NIO (the JVM, `gearsDemoJVM/run ...`). A demo that needs a capability
// asks for it, so one NIO lacks does not compile against it.
lazy val gearsDemo =
  crossProject(JVMPlatform, NativePlatform)
    .crossType(CrossType.Full)
    .in(file("gears-demo"))
    .settings(publish / skip := true)
    .jvmSettings(
      Compile / mainClass := Some("gearsexample.Main"),
      libraryDependencies += "com.lihaoyi" %% "mainargs" % "0.7.8",
      run / fork := true,
      // So that `-Dasyncio.gears.carriers=reactors` can carry the futures on the reactor pool (see `VirtualCarriers`).
      run / javaOptions ++= Seq(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/jdk.internal.vm=ALL-UNNAMED"
      )
    )
    .nativeSettings(nativeConfig ~= { _.withMultithreading(true) })
    .dependsOn(asyncioTestkit, higherLevel)
    .jvmConfigure(_.dependsOn(jvmAsyncioGears, jvmAsyncioNio))
    .nativeConfigure(_.dependsOn(nativeAsyncioGears, nativeAsyncioLoop))

// ported from https://github.com/bishabosha/scala-native-async-io/commit/6e8320d36c71f75c9daf85065f5c89729f0825c0
lazy val kqueueDemo =
  (project in file("kqueue-demo"))
    .enablePlugins(ScalaNativePlugin)
    .settings(
      publish / skip := true,
      Compile / mainClass := Some("example.Main"),
      libraryDependencies += "com.lihaoyi" %% "mainargs" % "0.7.8",
      // The gears demos run futures on a pool of worker threads; `gears bench --backend nwframework` links Network.framework.
      nativeConfig ~= { _.withMultithreading(true).withLinkingOptions(_ ++ networkFramework) }
    )
    .dependsOn(gearsDemo.native, nativeAsyncioNWFramework)

// The kqueueDemo bench scenarios written with cats-effect and fs2, to compare against; see kqueue-demo/bench/compare.py.
// The same code on Scala Native (kqueue, through build.sh) and on the JVM (its selector poller, through
// gears-demo/bench/build-jvm.sh).
lazy val ceBench =
  crossProject(JVMPlatform, NativePlatform)
    .crossType(CrossType.Full)
    .in(file("kqueue-demo/ce-bench"))
    .settings(
      publish / skip := true,
      Compile / mainClass := Some("cebench.CeBench"),
      libraryDependencies ++= Seq(
        "org.typelevel" %% "cats-effect" % "3.7.1",
        "co.fs2" %% "fs2-io" % "3.14.0",
        "com.lihaoyi" %% "mainargs" % "0.7.8"
      )
    )
    .nativeSettings(nativeConfig ~= { _.withMultithreading(true) })

// JVM tests that drive the linked kqueueDemo executable as a subprocess (macOS only)
lazy val kqueueDemoTests =
  (project in file("kqueue-demo/tests"))
    .settings(
      publish / skip := true,
      libraryDependencies += "org.scalameta" %% "munit" % "1.3.6" % Test,
      testFrameworks += MUnitFramework,
      Test / fork := true,
      Test / javaOptions += {
        val binary = fileConverter.value.toPath((kqueueDemo / Compile / nativeLink).value)
        s"-Dkqueue.demo.binary=${binary.toAbsolutePath}"
      },
      // The same gears demos on the JVM, started as `java -cp <this> gearsexample.Main`.
      Test / javaOptions += {
        val converter = fileConverter.value
        val classpath = (gearsDemo.jvm / Runtime / fullClasspath).value
          .map(entry => converter.toPath(entry.data).toAbsolutePath.toString)
          .mkString(java.io.File.pathSeparator)
        s"-Dgears.demo.jvm.classpath=$classpath"
      }
    )
