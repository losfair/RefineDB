#!/bin/bash

set -euo pipefail

TMP="/tmp/rdb-docker-build-`uuidgen`"
mkdir "$TMP"

cleanup()
{
    echo "Cleanup up temporary directory $TMP"
    rm -r "$TMP"
}

trap cleanup EXIT

cd "`dirname $0`"

cp ./target/release/rdb-server "$TMP/"
cp ./rdb-server.Dockerfile "$TMP/Dockerfile"
docker build --build-arg http_proxy --build-arg https_proxy -t "losfair/rdb-server" "$TMP"

echo "Build done."
