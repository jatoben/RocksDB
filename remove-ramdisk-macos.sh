#!/bin/bash
#
# Removes the temporary ramdisk used to store database files during a test run.
set -e

MOUNTPOINT="${1}"
[ -d "$MOUNTPOINT" ]

device=$(df "$MOUNTPOINT" 2>/dev/null | tail -n1 | grep "$MOUNTPOINT" | awk '{print $1}')
[ -n "$device" ]

umount "$MOUNTPOINT"
rmdir "$MOUNTPOINT"
hdiutil detach -quiet "$device"
