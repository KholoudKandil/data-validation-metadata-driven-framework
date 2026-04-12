# Changelog

## [1.1.0] - 2026-04-12

### Performance
- Cache DataFrame before OK/KO split in `transform_validate_fields` to avoid double full scan — reduces I/O and compute by 2x on validation runs

## [1.0.0] - 2026-03-29

### Added
- Explicit column MERGE logic for robust append operations
- UniqueKey configuration for deduplication
- Error handling for invalid Delta table states
- Validators registry pattern for extensibility

### Changed
- Refactored sinks.py with explicit column mapping
- Refactored validators.py with registry pattern
- Updated metadata.yaml with symmetric configuration

### Fixed
- Error message in apply_validations() now says "Field 'X' not found in DataFrame"

### Tests
- 35 comprehensive tests (100% passing)
