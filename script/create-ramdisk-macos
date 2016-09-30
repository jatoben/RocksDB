#!/bin/bash
#
# Creates a temporary ramdisk to store database files while running tests. This
# is fast and doesn't leave any trace on the filesystem when we're done.
set -e

MOUNTPOINT="${1}"
DISKSIZE=${2:-64}

mkdir -p "$MOUNTPOINT"
sector=$(expr $DISKSIZE \* 1024 \* 1024 / 512)
device="$(hdid -nomount "ram://${sector}" | awk '{print $1}')"

newfs_hfs "$device" > /dev/null
mount -t hfs "$device" "$MOUNTPOINT"
