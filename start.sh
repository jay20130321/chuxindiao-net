#!/usr/bin/env bash
# 启动楚新钓本地联机服务
cd "$(dirname "$0")"
if [ ! -d node_modules ]; then
  npm install
fi
echo "联机服务：ws://127.0.0.1:8787/ws"
exec node index.js
