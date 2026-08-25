#!/usr/bin/env bash
# InterGate 管理脚本(Termux / Linux 均可)
# 用法: ./run.sh [start|stop|restart|status]   默认 start
cd "$(dirname "$0")"
PID_FILE="intergate.pid"
PORT=51234
WEB_PORT=51235

# 端口占用检测: SO_REUSEADDR 避免 TIME_WAIT 误判
ports_used() {
  python3 - "$PORT" "$WEB_PORT" <<'PY' 2>/dev/null
import socket, sys
def used(p):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        s.bind(("0.0.0.0", p)); return False
    except OSError:
        return True
    finally:
        s.close()
sys.exit(0 if (used(int(sys.argv[1])) or used(int(sys.argv[2]))) else 1)
PY
}

is_running() {
  # 1) PID 文件
  if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
    return 0
  fi
  # 2) 端口被占用即视为已在运行
  if ports_used; then return 0; else return 1; fi
}

start() {
  if is_running; then
    echo "InterGate 已在运行(PID $(cat "$PID_FILE" 2>/dev/null || echo '?'))."
    echo "如端口被其他程序占用, 请先 ./run.sh stop; 强制重启: ./run.sh restart"
    return 0
  fi
  echo "启动 InterGate ..."
  nohup python3 main.py > intergate.log 2>&1 &
  echo $! > "$PID_FILE"
  sleep 4
  if is_running; then
    echo "已启动 (PID $(cat "$PID_FILE"))"
    echo "  网关     -> http://127.0.0.1:$PORT/v1"
    echo "  控制台   -> http://127.0.0.1:$WEB_PORT"
  else
    echo "启动失败, 请查看 intergate.log"; return 1
  fi
}

stop() {
  # 1) PID 文件精确停止
  if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
    kill -9 "$(cat "$PID_FILE")" 2>/dev/null
    rm -f "$PID_FILE"
    echo "已停止(PID 文件)"
  else
    # 2) 兜底: 按进程名清理
    local pids
    pids=$(pgrep -f "python3 .*main[.]py" 2>/dev/null)
    if [ -n "$pids" ]; then
      # shellcheck disable=SC2086
      kill -9 $pids 2>/dev/null
      rm -f "$PID_FILE"
      echo "已停止(按进程名清理)"
    else
      rm -f "$PID_FILE"
      echo "未在运行"
    fi
  fi
  # 等待端口释放(最多 15 秒), 避免 restart 时 bind 失败
  local i=0
  while [ $i -lt 15 ]; do
    if ! ports_used; then break; fi
    sleep 1; i=$((i+1))
  done
}

backup() {
  mkdir -p backups
  local ts
  ts=$(date +%Y%m%d-%H%M%S)
  local out="backups/intergate-${ts}.tgz"
  # 用 SQLite 在线备份 API, 无需停止服务(自动处理锁)
  python3 - "$out" <<'PY'
import sqlite3, sys, os
src = "data/intergate.db"
dst = os.path.join("backups", os.path.basename(sys.argv[1]).replace(".tgz", ".db"))
conn = sqlite3.connect(src)
bak = sqlite3.connect(dst)
with bak:
    conn.backup(bak)
bak.close(); conn.close()
print("db backed up:", dst)
PY
  if [ -f data/.master_key ]; then
    tar -czf "$out" -C . backups/"$(basename "$out" .tgz).db" data/.master_key 2>/dev/null
  else
    tar -czf "$out" -C . backups/"$(basename "$out" .tgz).db" 2>/dev/null
  fi
  rm -f "backups/$(basename "$out" .tgz).db"
  echo "备份完成 -> $out"
  ls -lh "$out" | awk '{print "  " $5 "  " $9}'
}

case "${1:-start}" in
  start)  start ;;
  stop)   stop ;;
  restart) stop; start ;;
  status)
    if is_running; then echo "运行中 (PID $(cat "$PID_FILE" 2>/dev/null || echo '?'))"; else echo "未运行"; fi ;;
  backup) backup ;;
  *) echo "用法: ./run.sh [start|stop|restart|status|backup]"; exit 1 ;;
esac
