#!/usr/bin/env bash

set -e

BASE=$GOPATH/src
GMAJ=github.com/r-medina/gmaj

for d in $(find $BASE/$GMAJ -name "*.proto" | xargs -n1 dirname | uniq); do
    protoc -I$BASE --go_out=plugins=grpc:$BASE $d/*.proto
done
