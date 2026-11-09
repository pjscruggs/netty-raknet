# Contributing

Base changes on `main`. Keep commits focused, use Conventional Commit messages,
and sign commits. Open pull requests with the behavior change and relevant
validation. Required CI checks must pass before updating the maintained branch.

## Local checks

Use JDK 25 and run `./gradlew build --no-daemon --max-workers=2 --warning-mode fail`
(Windows: `gradlew.bat`). `./gradlew -p quality check` runs the standalone Java
source policy, including its regression tests. All Java declarations, including
private members, need useful documentation. Methods, initializers, constructors,
and lambdas have a cyclomatic complexity limit of ten. There is no legacy
baseline or suppression exemption. Preserve Java 8 compatibility in production
code and exercise changes through the existing transport regression tests.

CI covers Linux and Windows, JDK 17/21/25 test runtimes, the older Netty API
baseline and the maintained Netty test runtime. The `Raknetify2 integration`
check builds all consumer modules against the candidate transport commit.
When updating that fixture, pin a reviewed Raknetify2 commit; do not use a
floating branch or copy the consumer's publication credentials into this repo.
Raknetify2's full native compatibility CI must pass before merging a changed
submodule pin there. Publishing the transport fork does not publish a mod.

CodeQL extracts against the declared Netty API baseline and runs the full
security-and-quality suite with Raknetify2's tested callback-aware unused
parameter query. Default interface callbacks preserve their extension signature;
ordinary unused parameters still fail. Query regression tests verify both cases.

## Licensing

Contributions are MIT licensed. Retain existing copyright and license notices;
do not replace upstream authors with the current maintainer. New source files
may use `SPDX-License-Identifier: MIT`. Add a copyright notice only for the
applicable contributor's work. Keep LICENSE and NOTICE in redistributed JARs.
The checks adapted from Raknetify2 retain their additional license in quality/.

## Reports and artifacts

Use minimal synthetic reproductions without credentials, user information, or
private operational data. Public CI uses fresh hosted runners and explicitly
selected build outputs. Keep private infrastructure measurements, captures,
profiles, logs, reports, and cost records outside Git history, issues, release
notes, and workflow artifacts. Do not upload a workspace directory wholesale.

Report suspected vulnerabilities through the private route in SECURITY.md.
