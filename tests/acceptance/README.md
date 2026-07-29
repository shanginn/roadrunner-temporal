## Acceptance Tests

This directory contains acceptance tests to verify the functionality of RoadRunner and PHP SDK integration with Temporal server.
The PHP SDK is included as a submodule in this repository.

### Setup

First, setup `sdk-php` Git submodule:

```bash
git submodule update --remote
```

Now `tests/acceptance/php-sdk` contains PHP SDK source code.

Next, to run the PHP SDK, you need to install PHP dependencies:

```bash
cd tests/acceptance/php-sdk
composer install
```

Next, you need to download the Temporal Dev Server and Temporal Test Server.
This is done using the DLoad utility, which comes bundled with the PHP SDK.
DLoad automatically fetches the correct binary versions from the configuration, which is especially useful since different PHP SDK branches may require different Temporal server versions (including pre-release builds with experimental features).

Download the binaries with the following command: (from `tests/acceptance/php-sdk`)

```bash
composer get:binaries
```

To build a reproducible local RoadRunner with the Temporal plugin from this
checkout, use the pinned direct-source builder:

```bash
./build-local.sh
```

It fetches RoadRunner `v2025.1.15` at immutable commit
`321b817fab1056e404533ca1ddd200e77d1525fd`, pins Go 1.26.4, replaces the
bundled Temporal plugin with this repository, resolves the module graph, and
writes `rr-nexus`. Override only when intentionally testing a different host
commit or output path:

```bash
ROADRUNNER_REF=321b817fab1056e404533ca1ddd200e77d1525fd \
ROADRUNNER_BINARY=/absolute/path/to/rr-nexus \
./build-local.sh
```

The exact binary used by acceptance tests is
`tests/acceptance/rr-nexus`. Point the PHP SDK at it with an absolute path to
avoid ambiguity:

```bash
export ROADRUNNER_BINARY="$(pwd)/rr-nexus"
```

DLoad/Velox remains available for release-matrix builds. It requires an
authenticated GitHub API token:

```bash
export DLOAD_BIN=./php-sdk/vendor/bin/dload
GITHUB_TOKEN=... CGO_ENABLED=0 "$DLOAD_BIN" build --config ./dload.xml
```

### Running Tests

Navigate to the php-sdk directory and run the Acceptance or Functional tests using Composer: (from `tests/acceptance/php-sdk`)

```bash
cd php-sdk
ROADRUNNER_BINARY='../rr-nexus' composer test:accept-fast
ROADRUNNER_BINARY='../rr-nexus' composer test:accept-slow
ROADRUNNER_BINARY='../rr-nexus' composer test:func
```
