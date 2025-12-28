# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0](https://github.com/elbaro/pyro-postgres/releases/tag/v0.2.0) - 2025-12-28

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
