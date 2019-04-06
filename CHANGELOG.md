# Changelog

## [Unreleased]


### Added
- New architecture of Daemon, Broker, Worker

### Changed
- Migrate from actix-web to tarpc for local, remote RPC
- Use json5 to parse CLI arguments
- Each worker has a capacity (v0.0.1 had a capacity constraint on each queue)

### Removed
- Queue

## [0.0.1]
- initial release
- works fine for local usage
