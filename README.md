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
Use `cargo install uplink` or `cargo install uplink-server`

## Usage
Generate an auth key using `uplink key-gen KEY`

Start a server using `uplink-server -b 127.0.0.1:4500 -s MY-STORAGE -a KEY`

Connect via client:
- Create config `uplink -s 127.0.0.1:4500 -a KEY init -n data -t important -d important -p cfg.toml --no-delete`
- Patch data `uplink push`
- Receive patches `uplink pull`
- Remove entry `uplink remove -n data`