# Contributing

## Building `gears`

The build uses sbt 2 and Scala 3.9.0 on all platforms. It requires:

- **JDK 21+** to run sbt and use virtual threads on the JVM.
- **On Scala Native**: Clang/LLVM 16+ and the [Scala Native prerequisites](https://scala-native.org/en/stable/user/setup.html). The build selects Scala Native 0.5.12.
- **For the `io` module on Linux**: liburing and OpenSSL development libraries, and the `openssl` command for the TLS tests, for example `sudo apt-get install liburing-dev libssl-dev openssl`. The io_uring backend needs Linux 5.11 or later.
- **On Scala.js**: Node.js 26+ to run the WebAssembly backend with JSPI. The build selects Scala.js 1.22.0.

All of the needed dependencies can be loaded by the included Nix Flake. If you have `nix` with `flake` enabled, run
```
nix develop
```
to enter the development environment with all the dependencies loaded. You can also use [direnv](https://direnv.net/)'s `use flake` to automate this process.

Once done, it should suffice to run
```bash
sbt publishLocal
```
to have Gears compiled and published locally for usage.

## Running Tests

```bash
sbt test
```

To test one platform, run `sbt rootJVM/test`, `sbt rootNative/test`, or `sbt rootJS/test`. The I/O module is tested
with `sbt ioJVM/test`, `sbt ioNative/test` and `sbt ioJS/test`, and its examples run with
`sbt 'nativeExamples/run tcp-echo'`.
With sbt 2, separate multiple commands with semicolons inside one quoted argument, for example:

```bash
sbt 'scalafmtCheckAll; test'
```
