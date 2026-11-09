# netty-raknet

[![Build](https://github.com/pjscruggs/netty-raknet/actions/workflows/build.yml/badge.svg?branch=main)](https://github.com/pjscruggs/netty-raknet/actions/workflows/build.yml)
[![CodeQL](https://github.com/pjscruggs/netty-raknet/actions/workflows/codeql.yml/badge.svg?branch=main)](https://github.com/pjscruggs/netty-raknet/actions/workflows/codeql.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A maintained RakNet transport library for Netty, used by
[Raknetify2](https://github.com/pjscruggs/raknetify2). This fork maintains
transport correctness, connection lifecycle handling, and compatibility with
the Netty versions supplied by supported Minecraft and proxy platforms.

It builds on [RelativityMC/netty-raknet](https://github.com/RelativityMC/netty-raknet)
and the original [yesdog/netty-raknet](https://github.com/yesdog/netty-raknet).
It is independently maintained by pjscruggs; upstream services and packages
do not represent builds of this fork.

## Features

- Netty client and server channels with Bootstrap and ServerBootstrap integration.
- Reliable, ordered delivery over UDP with acknowledgments and retransmission.
- Buffer recycling, fragmentation, reassembly, and Netty leak detection support.
- Channel writability for backpressure and bounded pending frame sets.
- Configurable packet identifiers, pipelines, and flush behavior.

## Build and test

Use JDK 25 and the checked-in Gradle wrapper:

```sh
git clone https://github.com/pjscruggs/netty-raknet.git
cd netty-raknet
./gradlew build --no-daemon --max-workers=2 --warning-mode fail
```

On Windows use `gradlew.bat` in place of `./gradlew`.
The build includes transport tests, documentation checks, a maximum cyclomatic
complexity of ten (including lambdas), and Java correctness checks. Binary,
source, and Javadoc JARs are written to each module's `build/libs/` directory;
each contains the license and attribution notices. These are thin library JARs:
Netty is supplied by the embedding application.

The production library targets Java 8 APIs and bytecode. Build tooling and
source checks run on JDK 25; CI additionally runs library tests on JDK 17 and 21.
Compilation retains the Netty 4.1.25 API baseline for older integrations.
Tests normally use Netty 4.1.140.Final, with a separate CI lane exercising the
older API baseline. The baseline is not a recommendation to deploy old Netty.
To select a test runtime without changing the compile baseline:

```sh
./gradlew test -PnettyTestVersion=4.1.140.Final --no-daemon
```

See [EndToEndTest.java](tests/src/test/java/network/ycc/raknet/EndToEndTest.java)
for client/server usage and [CONTRIBUTING.md](CONTRIBUTING.md) for checks.

## Use with Raknetify2

Raknetify2 includes this repository as a Git submodule and Gradle composite
build, pinned to an exact commit. Its Minecraft and proxy compatibility tests
remain in that repository. This fork's CI also builds a pinned Raknetify2
revision with the exact proposed transport commit before a dependency update.
The integration fixture is recorded in `.github/workflows/build.yml` and should
be advanced deliberately when Raknetify2 changes.

There is no standalone Maven release channel for this fork yet. The inherited
`network.ycc` / `0.8-SNAPSHOT` coordinates are internal build metadata, not a
published distribution of these changes. Use the source/composite build;
upstream CodeMC artifacts do not contain this fork's fixes. Any future public
Maven releases will use a maintainer-owned namespace and explicit versions.

## Support and security

Report library bugs in [this repository's issues](https://github.com/pjscruggs/netty-raknet/issues).
Report Minecraft or proxy integration issues in
[Raknetify2](https://github.com/pjscruggs/raknetify2/issues).
For vulnerabilities, follow [SECURITY.md](SECURITY.md) instead of opening a public issue.

## License and attribution

[MIT](LICENSE). Preserve the original YCC Network copyright and permission
notice when redistributing the code. See [NOTICE](NOTICE) for project provenance
and the source-quality rules' attribution. New contributions retain the same
license; existing authorship and upstream history remain intact.
