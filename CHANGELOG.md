# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.0](https://github.com/elbaro/pyro-postgres/compare/v0.3.2...v0.4.0) (2026-04-19)


### ⚠ BREAKING CHANGES

* update actions/upload-pages-artifact action to v5 ([#20](https://github.com/elbaro/pyro-postgres/issues/20))
* update breaking dependencies ([#14](https://github.com/elbaro/pyro-postgres/issues/14))

* update actions/upload-pages-artifact action to v5 ([#20](https://github.com/elbaro/pyro-postgres/issues/20)) ([3747dd3](https://github.com/elbaro/pyro-postgres/commit/3747dd38dda2b28a75a4ce9d7da040fae8fdb13a))
* update breaking dependencies ([#14](https://github.com/elbaro/pyro-postgres/issues/14)) ([94386ef](https://github.com/elbaro/pyro-postgres/commit/94386efa86153a05835ade58bd21890bd317eeea))

## [0.3.2](https://github.com/elbaro/pyro-postgres/compare/v0.3.1...v0.3.2) (2026-03-03)


### Bug Fixes

* move separateMajorMinor to top-level config ([6c213c1](https://github.com/elbaro/pyro-postgres/commit/6c213c14f132b4203d91c4ae6b4a31987ebc8e39))

## [0.3.1](https://github.com/elbaro/pyro-postgres/compare/v0.3.0...v0.3.1) (2025-12-30)


### Bug Fixes

* apply ci patches from pyro-mysql ([e2dfcad](https://github.com/elbaro/pyro-postgres/commit/e2dfcad56914c5f1964ff1453ed030b8172b76d0))
* **ci:** add openssl in linux, and use python 3.10 in windows ([2d4d532](https://github.com/elbaro/pyro-postgres/commit/2d4d532186e684b1b0473e5c6394f44493f882a6))
* **ci:** use debian command, not centos ([e14d9d0](https://github.com/elbaro/pyro-postgres/commit/e14d9d09ee85a647d95d7d628a0c435f43eedf51))

## [0.3.0](https://github.com/elbaro/pyro-postgres/compare/pyro_postgres-v0.2.1...pyro_postgres-v0.3.0) (2025-12-29)


### ⚠ BREAKING CHANGES

* rename prefer_unix_socket to upgrade_to_unix_socket

* rename prefer_unix_socket to upgrade_to_unix_socket ([2531956](https://github.com/elbaro/pyro-postgres/commit/253195637b79992190840a95596924932034973f))


### Features

* conn.exec_drop() returns affected_rows ([8dba8b5](https://github.com/elbaro/pyro-postgres/commit/8dba8b5152535a691f6fca0e11d26ed33d82abd2))
* exec_* accepts PreparedStatement as well ([ffb6c6d](https://github.com/elbaro/pyro-postgres/commit/ffb6c6de4c8296a734d851f9a8b6324e0a04458e))
* **test:** test simple and extended query methods ([4ecf180](https://github.com/elbaro/pyro-postgres/commit/4ecf18047a152a3bca82efe4524df223a23f33fa))


### Bug Fixes

* params default to (), not None ([4352eab](https://github.com/elbaro/pyro-postgres/commit/4352eabe5e9dae013c704699844655607acdef42))
* **test:** remove outdated affected_rows tests ([ff1fe21](https://github.com/elbaro/pyro-postgres/commit/ff1fe2129a8fcce8871421037b4fea27cede1932))
* wrong init() arguments ([0f0bca7](https://github.com/elbaro/pyro-postgres/commit/0f0bca7b3018f385922c754b617ffa95e1b97aba))


### Documentation

* add pages ([b70924b](https://github.com/elbaro/pyro-postgres/commit/b70924b2b0c1637ce9542811575db815ba65e193))
* conn ([dfb7294](https://github.com/elbaro/pyro-postgres/commit/dfb729426a22b20843b23f86c2575cc56a528488))
* pipelining ([69a2ad5](https://github.com/elbaro/pyro-postgres/commit/69a2ad538c83086784b78dbd0dedd36270ce7549))
* query ([1c95dcc](https://github.com/elbaro/pyro-postgres/commit/1c95dcc8ddf78113a37fdada3f117502b5e29c41))

## [Unreleased]

## [0.2.1](https://github.com/elbaro/pyro-postgres/releases/tag/v0.2.1) - 2025-12-28

### Added

- exec_* accepts PreparedStatement as well
- *(test)* test simple and extended query methods
- conn.exec_drop() returns affected_rows

### Fixed

- params default to (), not None
- *(test)* remove outdated affected_rows tests

### Other

- release-please to release-plz
- test new wheel build
- add release-please permissions
- change release PR title
- add maturin
- rename ci.yml to test.yml
- remove duplicated tests
- bump
- rename portal.execute_* to portal.exec_*. portal.exec_* doesn't not accept conn
- format tests
- add black to pre-commit
- add github workflows
- pre-commit hooks
- docs
- pipelining
- query
- conn
- docs
- docs
- docs
- Create book.yml
- fmt
- render svg
- fmt
- test htmlpreview fix
- Fix links
- Update bench
- Add async pipeline
- Support Uuid, Decimal; Add tests
- Update bench with `CREATE TEMP TABLE`
- initial bench
- Fix de/serialization logic; Add tests
- clippy
- Fix the use of pyo3 deprecated API
- init
