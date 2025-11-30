# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- Handle and log query errors in type server by @tsloughter in [#106](https://github.com/erleans/pgo/pull/106)

## [0.15.0] - 2025-07-01

### Changed
- Add support for pg_timestampz with UTC offsets by @chiroptical in [#98](https://github.com/erleans/pgo/pull/98)
- Export query/4 to support manually passing connection by @tsloughter in [#95](https://github.com/erleans/pgo/pull/95)
- Add test of domain_socket support by @tsloughter in [#94](https://github.com/erleans/pgo/pull/94)
- Forgot to update ssl postgres docker image by @tsloughter in [#93](https://github.com/erleans/pgo/pull/93)
- Update otel api library by @tsloughter in [#91](https://github.com/erleans/pgo/pull/91)
- Handle domain socket host names by @cmkarlsson in [#90](https://github.com/erleans/pgo/pull/90)
- Fix typespecs by @asabil in [#67](https://github.com/erleans/pgo/pull/67)
- Time zone should be explicitly set for timestamp related tests by @chiroptical in [#89](https://github.com/erleans/pgo/pull/89)
- Update CI by @chiroptical in [#85](https://github.com/erleans/pgo/pull/85)
- Add ensure_all_started to make it obvious you need the application by @chiroptical in [#73](https://github.com/erleans/pgo/pull/73)
- Update github actions runner to ubuntu 22.04 by @chiroptical in [#74](https://github.com/erleans/pgo/pull/74)
- Fix pgo_handler:decode_tag return value by @asabil in [#64](https://github.com/erleans/pgo/pull/64)
- Remove telemetry from lock file and update license string by @tsloughter

## New Contributors
* @chiroptical made their first contribution in [#98](https://github.com/erleans/pgo/pull/98)
* @cmkarlsson made their first contribution in [#90](https://github.com/erleans/pgo/pull/90)
## [0.13.0] - 2022-10-13

### Changed
- Add testing of transaction/2 by @tsloughter
- Fix infinite loop in pgo:transaction/2 by @asabil in [#63](https://github.com/erleans/pgo/pull/63)
- OpenTelemetry replaces OpenCensus for tracing by @tsloughter in [#62](https://github.com/erleans/pgo/pull/62)

## New Contributors
* @asabil made their first contribution in [#63](https://github.com/erleans/pgo/pull/63)
## [0.12.0] - 2022-06-07

### Changed
- Add logging when connecting fails by @tsloughter in [#59](https://github.com/erleans/pgo/pull/59)
- Update github actions to point to main branch by @tsloughter
- Support for SCRAM-SHA-256 authentication by @tsloughter in [#58](https://github.com/erleans/pgo/pull/58)
- Permit passing of socket options by @lpil in [#55](https://github.com/erleans/pgo/pull/55)
- Upgrade pg_types to hex package 0.4.0 by @tsloughter in [#45](https://github.com/erleans/pgo/pull/45)
- Upgrade pg_types to support micro second timestamps by @tsloughter
- Switch pg_types to master branch for system timestamp by @tsloughter
- Add table as possible return type for num_rows by @CrowdHailer in [#43](https://github.com/erleans/pgo/pull/43)
- Handle transaction rollback by @benbro in [#40](https://github.com/erleans/pgo/pull/40)
- Upgrade pg_types to 0.3.0 by @tsloughter
- Add test for pgo_pool:disconnect/4 by @tsloughter
- Upgrade pg_types and add format_error/1 to format encoding failures by @tsloughter
- Upgrade pg_types to support decoding intervals by @tsloughter
- Upgrade pg_types and add test for infinity in time ranges by @tsloughter
- Permit start_link to take proplists + binaries by @lpil in [#33](https://github.com/erleans/pgo/pull/33)
- Correct erlang version requirement by @tsloughter

### Fixed
- Fix match on ets:lookup in delete_holder to be on 5-tuple by @tsloughter

## New Contributors
* @CrowdHailer made their first contribution in [#43](https://github.com/erleans/pgo/pull/43)
* @benbro made their first contribution in [#40](https://github.com/erleans/pgo/pull/40)
## [0.11.0] - 2019-12-15

### Changed
- Bump pg_types to 0.2.0 by @tsloughter
- Fail on purpose by @tsloughter
- Include trace configuration in README example by @lpil in [#28](https://github.com/erleans/pgo/pull/28)
- Add surefire hook by @tsloughter

## New Contributors
* @lpil made their first contribution in [#28](https://github.com/erleans/pgo/pull/28)
## [0.10.0] - 2019-08-27

### Changed
- Update pg_types for tsvector support by @tsloughter
- Upgrade pg_types by @tsloughter
- Null support in records by @tsloughter
- Configurable uuid decode by @tsloughter
- Update geometric tests by @tsloughter
- Use new orb with overridable executor by @tsloughter
- Add array null element test by @tsloughter
- Update pg_types by @tsloughter
- Update pgtypes by @tsloughter
- Update pgtypes for json encode/decode by @tsloughter
- Add tests for tid and bit_string by @tsloughter
- Add tests for types circle, line, line_segment, path and polygon by @tsloughter
- Upgrade pg_types to support records/composite types by @tsloughter
- Reraise type find error if params are equal length by @tsloughter
- Update pg_types: hstore support by @tsloughter
- Add empty range tests by @tsloughter
- Upgrade pg_types and add more range tests by @tsloughter
- Pg types by @tsloughter in [#22](https://github.com/erleans/pgo/pull/22)

### Fixed
- Fix starting of postgres-ssl by @tsloughter

## [0.8.0] - 2019-05-27

### Changed
- Upgrade telemetry by @tsloughter in [#20](https://github.com/erleans/pgo/pull/20)
- Remove dead function clause for parsing exponents by @starbelly in [#13](https://github.com/erleans/pgo/pull/13)

### Fixed
- Fix ssl support by @tsloughter in [#17](https://github.com/erleans/pgo/pull/17)

## [0.7.0] - 2019-05-02

### Changed
- Re-work numeric encoding and decoding by @starbelly in [#11](https://github.com/erleans/pgo/pull/11)
- Implement ping of connection through sync command by @tsloughter

## New Contributors
* @starbelly made their first contribution in [#11](https://github.com/erleans/pgo/pull/11)
## [0.6.2] - 2019-02-08

### Changed
- Add to docs how to start pool in supervisor by @tsloughter

## [0.6.1] - 2019-02-08

### Changed
- Add overview doc by @tsloughter

## [0.6.0] - 2019-01-28

### Changed
- Add support for startup connection parameters by @tsloughter
- Some name cleanup by @tsloughter
- Updates to how configuration is handled by @tsloughter
- Support query options set per-pool by @tsloughter
- Add test for return_rows_as_maps by @tsloughter
- Some cleanup by @tsloughter
- Just some formatting by @tsloughter
- Add note about opencensus being optional by @tsloughter
- Update readme for telemetry and tracing by @tsloughter
- Optional opencensus tracing by @tsloughter
- Execute telemetry event and fixup transactions by @tsloughter
- Add initial telemetry event execute by @tsloughter
- Updated requirements in readme by @tsloughter
- Cleanup api some and remove opencensus for now by @tsloughter
- Example code for doing transaction. in [#10](https://github.com/erleans/pgo/pull/10)
- Update rebar3 orb by @tsloughter
- Type load loop by @tsloughter in [#9](https://github.com/erleans/pgo/pull/9)

### Fixed
- Fix hex badge by @tsloughter
- Fix xref run by @tsloughter
- Fix doc generation by @tsloughter
- Fix git repo address by @getong in [#7](https://github.com/erleans/pgo/pull/7)

## New Contributors
* @ made their first contribution in [#10](https://github.com/erleans/pgo/pull/10)
* @getong made their first contribution in [#7](https://github.com/erleans/pgo/pull/7)
## [0.5.0] - 2018-11-05

### Changed
- Move more eunit tests to common test by @tsloughter
- Remove simple query and text decoding by @tsloughter

## [0.4.0] - 2018-11-02

### Changed
- Upgrade opencensus to 0.6.0 by @tsloughter
- Support otp-21 by @tsloughter
- Run eunit in separate command in circleci (opencensus merl issue) by @tsloughter
- Update pool by @tsloughter
- Add requirements section to readme by @tsloughter

### Fixed
- Fix up support for datetime and interval while moving tests to ct by @tsloughter
- Fix running eunit after ct in same rebar3 command by @tsloughter
- Fix connection lookup by @tsloughter

## [0.3.0] - 2018-05-31

### Changed
- Upgrade opencensus dep by @tsloughter
- Return uuid as 16 byte binary by @tsloughter
- Replace pg_result record with map by @tsloughter in [#2](https://github.com/erleans/pgo/pull/2)
- Temporary commenting out of spans by @tsloughter
- Handle notice messages by @tsloughter

### Fixed
- Fix flush on bind error by @tsloughter

## [0.2.0] - 2018-03-12

### Changed
- Loosen hex dep constraints by @tsloughter
- Use git tags for versions by @tsloughter
- Upgrade opencensus to 0.3.0 by @tsloughter
- Trace transactions by @tsloughter
- Add opencensus tracing by @tsloughter
- Add covertool to output for codecov by @tsloughter
- Add codecov badge by @tsloughter
- Try codecov instead of coveralls that never works by @tsloughter
- Update app metadata by @tsloughter
- Add coveralls plugin by @tsloughter
- Type clean up by @tsloughter
- Improve error responses and add circle config by @tsloughter
- Add transaction function by @tsloughter
- Remove locked jsx by @tsloughter
- Update readme example by @tsloughter
- Simplify config by @tsloughter
- Export #pg_result{} type by @tsloughter
- Quick and dirty query cache by @tsloughter
- Few badmatch bugs in pool by @tsloughter
- Match on bigint tag to encode big integers by @tsloughter
- Update replacing sbroker with builtin pool impl from db_connection by @tsloughter
- Cleanup by @tsloughter
- Add some types by @tsloughter
- Add some types by @tsloughter
- Reload types when not found from row description by @tsloughter
- Enable more tests by @tsloughter
- Rework with forked pgsql protocol by @tsloughter
- Update metadata by @tsloughter
- Initial commit by @tsloughter

### Fixed
- Fix up automatic encoding of uuid and json by @tsloughter
- Fixup connection by @tsloughter

## New Contributors
* @tsloughter made their first contribution
[unreleased]: https://github.com/erleans/pgo/compare/v0.15.0..HEAD
[0.15.0]: https://github.com/erleans/pgo/compare/v0.13.0..v0.15.0
[0.13.0]: https://github.com/erleans/pgo/compare/v0.12.0..v0.13.0
[0.12.0]: https://github.com/erleans/pgo/compare/v0.11.0..v0.12.0
[0.11.0]: https://github.com/erleans/pgo/compare/v0.10.0..v0.11.0
[0.10.0]: https://github.com/erleans/pgo/compare/v0.8.0..v0.10.0
[0.8.0]: https://github.com/erleans/pgo/compare/v0.7.0..v0.8.0
[0.7.0]: https://github.com/erleans/pgo/compare/v0.6.2..v0.7.0
[0.6.2]: https://github.com/erleans/pgo/compare/v0.6.1..v0.6.2
[0.6.1]: https://github.com/erleans/pgo/compare/v0.6.0..v0.6.1
[0.6.0]: https://github.com/erleans/pgo/compare/v0.5.0..v0.6.0
[0.5.0]: https://github.com/erleans/pgo/compare/v0.4.0..v0.5.0
[0.4.0]: https://github.com/erleans/pgo/compare/v0.3.0..v0.4.0
[0.3.0]: https://github.com/erleans/pgo/compare/v0.2.0..v0.3.0

<!-- generated by git-cliff -->
