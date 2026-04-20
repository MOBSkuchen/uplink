# Uplink
Directory synchronization

Includes **uplink** (client) and **uplink-server**.

## Features
- Authentication
- High speed
- Pretty CLI
- Transfer optimizations
  - Diff only transfer
  - Fast Compression

## Installation
Use `cargo install uplink-sync` or `cargo install uplink-sync-server`

## Usage
Generate an auth key using `uplink-sync key-gen KEY`

Start a server using `uplink-sync-server -b 127.0.0.1:4500 -s MY-STORAGE -a KEY`

Connect via client:
- Create config `uplink-sync -s 127.0.0.1:4500 -a KEY init -n data -t important -d important -p cfg.toml --no-delete`
- Patch data `uplink-sync push`
- Receive patches `uplink-sync pull`
- Remove entry `uplink-sync remove -n data`