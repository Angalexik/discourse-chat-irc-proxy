#!/bin/bash

DIR="$(dirname -- "${BASH_SOURCE[0]}")"
DIR="$(realpath -e -- "$DIR")"

curl https://raw.githubusercontent.com/discourse/discourse/refs/heads/main/frontend/pretty-text/addon/emoji/data.js > "$DIR"/data/emoji.js
