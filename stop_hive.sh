#!/bin/bash
cd "$(dirname "$0")"
[ -f .hive/hiveserver2.pid ] && kill $(cat .hive/hiveserver2.pid) 2>/dev/null
pkill -f "org.apache.hive.service.server.HiveServer2" 2>/dev/null
rm -f .hive/hiveserver2.pid; echo ">> HiveServer2 stopped"
