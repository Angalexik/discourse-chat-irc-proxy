# Keysmash (ghffdsa)

Discourse chatroom to IRC proxy server

## Installation

Ensure you have [The Rust Toolchain](https://rust-lang.org/learn/get-started/) installed

Compile the binary with either

```bash
cargo build --release
```

to build in release mode with optimizations, or

```bash
cargo build
```

to build in debug mode.

Then, depending on whether the program was compiled in debug or release mode, copy either `target/debug/ghffdsa` or `target/release/ghffdsa`
to a directory that's within your `$PATH` (typically `~/.local/bin/`, `/usr/local/bin/`, or `/usr/bin/`)

## Usage

Run the program once and then close it to generate a default configuration file located at `~/.config/ghffdsa/config.toml`.

Replace the default config values with appropriate ones. Please note that `channel_name` and `channel_number` are hardcoded
to *"#blanket-fort"* and *4* respectively and changing the config values will have no actual effect.

After configuring the config file and restarting the server, you may use any IRC client to connect to the proxy server on port 6667

## Features

### Implemented

- Reading message backlog
- Reading new messages as they come in
- Sending messages
- User list

### Unimplemented

- Message replies
- Message reactions
- Message edits
- Message deletions
- Accurate timestamps for messages in the backlog
- `JOIN` and `PART` sent to the client as users enter and exit the chatroom
- Graceful error handling
- Approximately 80% of the required IRC server spec (none of the clients I've tested seem to mind, though)
