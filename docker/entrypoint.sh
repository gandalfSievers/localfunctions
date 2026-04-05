#!/bin/sh
set -e

# exec replaces the shell with the binary, making it PID 1 so it
# receives SIGTERM/SIGINT directly from Docker — no trap needed.
exec /usr/local/bin/localfunctions "$@"
