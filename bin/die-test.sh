#!/bin/bash

while true; do
  $* &
  pid=$!
  (sleep 70; kill -9 $pid &>/dev/null ) &
  wait $pid && break
done
