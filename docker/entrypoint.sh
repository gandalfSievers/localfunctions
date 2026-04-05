#!/bin/sh
set -e

# Signal forwarding — ensure clean shutdown
trap 'kill -TERM $PID; wait $PID' TERM INT

exec /usr/local/bin/localfunctions "$@"
