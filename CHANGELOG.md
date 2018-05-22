# Changelog

## [Unreleased]

### Added

- Bintray support
- Changelog
- Moved to H2 to test PostgreSQL compat
- Only use named parameters
- Changed namespace to `com.mrcsparker`
- Added `USE_JDBC_TYPES` to override `ResultSetRecordSet`. Hit an issue returning array types in NiFi and this fixes it.

### Changed

- Cleaned up build documentation

## [1.6.0-0] - 2018-05-08

### Added

- Base for new documentation
- Named parameter support
- Accepts JSON as input (for named parameters)

### Changed
- Links to NiFi articles

### Removed
- Removed SQLRecordSet. Using built-in NiFi Record support

## [1.5.0-1] - 2018-03-21

### Added
- Initial release
