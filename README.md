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

### Note about IRCv3

IRC clients using the Java library PircBotX implement the CAP command incorrectly.

In order to use these clients with the server, set `ircv3 = false` in the config file. If you use a client that implements CAP *correctly*, or a client that doesn't support IRCv3 at all, you can keep `ircv3 = true`.

## Features

### Implemented

- Reading message backlog
- Reading new messages as they come in
- Sending messages
- User list
- Accurate timestamps for messages in the backlog (with IRCv3)
- Message replies (with IRCv3)
- Message reactions (with IRCv3)
- `JOIN` and `PART` sent to the client as users enter and exit the chatroom (disabled by default)

### Unimplemented

- Message edits
- Message deletions
- Graceful error handling
- Approximately 80% of the required IRC server spec (none of the clients I've tested seem to mind, though)

## Bugs

- discourse usernames can contain dots but irc nicks can't should probably fix that
