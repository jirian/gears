import org.scalajs.jsenv.nodejs.*
import org.scalajs.linker.interface.ESVersion
import sbtcrossproject.CrossPlugin.autoImport.{CrossType, crossProject}

scalaVersion := "3.9.0"

// The Scala Native nightly selected in project/plugins.sbt brings its libraries from the snapshot repository.
ThisBuild / resolvers ++= (if (sys.env.get("SCALA_NATIVE_NIGHTLY").contains("1")) Seq(Resolver.sonatypeCentralSnapshots)
                           else Nil)

LocalRootProject / publish / skip := true

val MUnitFramework = new TestFramework("munit.Framework")

lazy val isLinux = System.getProperty("os.name").toLowerCase.contains("linux")

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

// The platform-independent reactor model: ops, completions, the Reactor trait, and the blocker pool.
lazy val asyncio =
  crossProject(JVMPlatform, NativePlatform)
    .crossType(CrossType.Pure)
    .in(file("asyncio"))
    .settings(
      publish / skip := true,
      libraryDependencies += "org.scalameta" %% "munit" % "1.3.6" % Test,
      testFrameworks += MUnitFramework
    )

// ported from https://github.com/bishabosha/scala-native-async-io/commit/6e8320d36c71f75c9daf85065f5c89729f0825c0
lazy val nativeAsyncioLoop =
  (project in file("asyncio-native/loop"))
    .enablePlugins(ScalaNativePlugin)
    .settings(publish / skip := true)
    .dependsOn(asyncio.native)

lazy val io =
  crossProject(JSPlatform, JVMPlatform, NativePlatform)
    .crossType(CrossType.Full)
    .in(file("io"))
    .settings(
      name := "gears-io",
      publish / skip := true,
      libraryDependencies += "org.scalameta" %% "munit" % "1.3.6" % Test,
      testFrameworks += MUnitFramework
    )
    .jvmSettings(scalacOptions += "-release:21")
    .nativeSettings(
      Test / unmanagedSources / excludeFilter := {
        val default = (Test / unmanagedSources / excludeFilter).value
        if (isLinux) default
        else
          default || "UringReactorSuite.scala" || "UringNetSuite.scala" || "HttpSuite.scala" ||
          "UringFileSuite.scala" || "DriverLifecycleSuite.scala" || "CancelRetrySuite.scala" ||
          "UringReadinessFallbackSuite.scala" || "BlockingPoolSuite.scala" || "TestReactor.scala" ||
          "ReactorDriverNetSuite.scala" || "ReactorDriverHttpSuite.scala" || "FileSuite.scala" || "ExternalDriverSuite.scala"
      },
      nativeConfig ~= { _.withMultithreading(true) }
    )
    .jsSettings(
      scalaJSLinkerConfig := (root.js / scalaJSLinkerConfig).value,
      jsEnv := Def.uncached((root.js / jsEnv).value)
    )
    .dependsOn(root)
    .nativeConfigure(_.dependsOn(nativeAsyncioLoop))

lazy val nativeExamples =
  (project in file("native-examples"))
    .enablePlugins(ScalaNativePlugin)
    .settings(
      publish / skip := true,
      Compile / mainClass := Some("gears.async.asyncio.examples.Main"),
      nativeConfig ~= { _.withMultithreading(true) }
    )
    .dependsOn(io.native)
