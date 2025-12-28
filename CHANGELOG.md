# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0](https://github.com/elbaro/pyro-postgres/compare/v0.1.0...v0.2.0) (2025-12-28)


### Features

* conn.exec_drop() returns affected_rows ([8dba8b5](https://github.com/elbaro/pyro-postgres/commit/8dba8b5152535a691f6fca0e11d26ed33d82abd2))
* exec_* accepts PreparedStatement as well ([ffb6c6d](https://github.com/elbaro/pyro-postgres/commit/ffb6c6de4c8296a734d851f9a8b6324e0a04458e))
* **test:** test simple and extended query methods ([4ecf180](https://github.com/elbaro/pyro-postgres/commit/4ecf18047a152a3bca82efe4524df223a23f33fa))


### Bug Fixes

* params default to (), not None ([4352eab](https://github.com/elbaro/pyro-postgres/commit/4352eabe5e9dae013c704699844655607acdef42))
* **test:** remove outdated affected_rows tests ([ff1fe21](https://github.com/elbaro/pyro-postgres/commit/ff1fe2129a8fcce8871421037b4fea27cede1932))
* wrong init() arguments ([0f0bca7](https://github.com/elbaro/pyro-postgres/commit/0f0bca7b3018f385922c754b617ffa95e1b97aba))

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
- change relase PR title
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
