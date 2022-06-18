# Changelog

## [Unreleased]

### Added

- SCRAM support -- Thanks to https://github.com/epgsql/epgsql/
- OpenTelemetry replaces OpenCensus for tracing
- Tracing is now enabled by default. Since Span operations are no-op's in
  `opentelemetry_api` if no SDK (the `opentelemetry` package) is started this
  doesn't change anything for anyone not using OpenTelemetry.
- `include_statement_span_attribute` option is added for enabling/disabling the
  inclusion of the whole query as an attribute on the OpenTelemetry Span started
  for a query.
- Create child spans for queries in a transaction

### Changed

### Removed
