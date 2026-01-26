#!/bin/bash
###############################################################################
# iperf3_benchmark.sh
#
# ─────────────────────────  NETWORK THROUGHPUT BENCHMARK  ─────────────────────
#
#  ▸ Goal
#      Measure aggregate TCP throughput from a set of *client* nodes to a
#      single *server* node inside a cluster/cloud testbed.
#
#  ▸ How it works
#      1.  Verifies the SERVER_IP belongs to the host running the script.
#      2.  Pings / SSH-checks every client to ensure password-less access.
#      3.  Installs iperf3 (via yum) on server + clients if missing.
#      4.  Starts one iperf3 server per client on consecutive ports:
#            BASE_PORT, BASE_PORT+1, …                    (background jobs)
#      5.  Launches iperf3 *clients* on all nodes in parallel via SSH,
#         each targeting its dedicated port and running with -P $STREAMS.
#      6.  Waits **only** for the SSH client jobs (not the servers).
#      7.  Parses each client log to print a clean summary table.
#      8.  Kills all iperf3-server processes.
#
#  ▸ Usage
#        • Edit SERVER_IP and CLIENTS arrays below.
#        • Run on the server node:  ./iperf3_benchmark.sh
#        • All results/diagnostics saved in /tmp:
#              /tmp/iperf3_server_<port>.log
#              /tmp/iperf3_result_<client>.log
#
#  ▸ Assumptions / Requirements
#        • Password-less SSH from server → clients.
#        • Ports BASE_PORT .. BASE_PORT+N open in any firewalls.
#        • RHEL/CentOS-style package manager (yum).
#        • Bash 4+, iperf3.
#
###############################################################################

# ─── USER CONFIGURATION ──────────────────────────────────────────────────────
SERVER_IP="10.10.1.1"                       # IP on *this* (server) node
CLIENTS=("10.10.1.2" "10.10.1.3")           # Client node IPs
#CLIENTS=( $(seq -f "10.10.1.%g" 2 33) )
BASE_PORT=5050                              # First iperf3-server port
PARALLEL_STREAMS=1                          # -P value for iperf3 client
DURATION=60                                 # Seconds each test runs
# ----------------------------------------------------------------------------

PING_TIMEOUT=3
SSH_TIMEOUT=5

###############################################################################
# Helper functions
###############################################################################

validate_server_ip() {
  ip addr show | grep -q "$SERVER_IP" || {
      echo "[ERROR] SERVER_IP ($SERVER_IP) not found on local interfaces."; exit 1; }
  echo "[✓] SERVER_IP ($SERVER_IP) is valid on this node."
}

check_connectivity() {
  echo "[*] Checking connectivity and SSH access..."
  for c in "${CLIENTS[@]}"; do
    echo " - $c"
    ping -c1 -W"$PING_TIMEOUT" "$c"  >/dev/null \
      || { echo "[ERR] ping $c failed"; exit 1; }
    ssh -oBatchMode=yes -oConnectTimeout="$SSH_TIMEOUT" "$c" 'echo SSH OK' \
      >/dev/null 2>&1 || { echo "[ERR] SSH $c failed"; exit 1; }
  done
  echo "[✓] All clients reachable via SSH."
}

ensure_iperf3_installed() {
  local host=$1
  local required_version="3.15"

  if [[ $host == "localhost" ]]; then
    current_version=$(iperf3 --version 2>/dev/null | awk '{print $2}' | head -1)
    if [[ "$current_version" != "$required_version" ]]; then
      echo "[*] Installing iperf3 v${required_version} on localhost..."

      sudo yum install -y gcc make wget tar libuuid-devel || { echo "yum install failed"; exit 1; }

      cd /tmp
      wget https://github.com/esnet/iperf/archive/refs/tags/${required_version}.tar.gz -O iperf3.tar.gz
      tar -xf iperf3.tar.gz
      cd iperf-${required_version}/
      ./configure
      make -j$(nproc)
      sudo make install

      echo "/usr/local/lib" | sudo tee /etc/ld.so.conf.d/iperf3.conf
      sudo ldconfig

      echo "[✓] iperf3 v${required_version} installed on localhost."
    else
      echo "[✓] iperf3 v${required_version} already installed on localhost."
    fi
  else
    echo "[*] Installing iperf3 v${required_version} on $host..."

    ssh "$host" bash -s <<EOF
      current_version=\$(iperf3 --version 2>/dev/null | awk '{print \$2}' | head -1)
      if [[ "\$current_version" != "$required_version" ]]; then
        sudo yum install -y gcc make wget tar libuuid-devel || exit 1
        cd /tmp
        wget https://github.com/esnet/iperf/archive/refs/tags/${required_version}.tar.gz -O iperf3.tar.gz
        tar -xf iperf3.tar.gz
        cd iperf-${required_version}/
        ./configure
        make -j\$(nproc)
        sudo make install
        echo "/usr/local/lib" | sudo tee /etc/ld.so.conf.d/iperf3.conf
        sudo ldconfig
      fi
EOF

    echo "[✓] iperf3 v${required_version} ensured on $host."
  fi
}

start_iperf3_servers() {
  echo "[*] Launching iperf3 servers on $SERVER_IP..."
  local i3; i3=$(command -v iperf3)
  [[ -z $i3 ]] && { echo "[ERR] iperf3 not in PATH"; exit 1; }

  for i in "${!CLIENTS[@]}"; do
    port=$((BASE_PORT+i))
    log="/tmp/iperf3_server_${port}.log"
    echo "   • port $port"
    nohup "$i3" -s -p "$port" >"$log" 2>&1 &
    sleep 0.3
    lsof -i :"$port" | grep -q LISTEN || {
        echo "[ERR] server failed on $port (see $log)"; exit 1; }
  done
  echo "[✓] Servers listening."
}


start_clients() {
  local client_pids=()

  echo "[*] Starting DOWNLOAD test for all clients..."
  for i in "${!CLIENTS[@]}"; do
    client=${CLIENTS[$i]}
    port=$((BASE_PORT + i))
    ssh "$client" \
      "iperf3 -c $SERVER_IP -p $port -P $PARALLEL_STREAMS -t $DURATION" \
      >"/tmp/iperf3_result_${client}_download.log" 2>&1 &
    client_pids+=($!)
  done
  for pid in "${client_pids[@]}"; do wait "$pid"; done
  echo "[✓] Download phase complete."

  client_pids=()
  echo "[*] Starting UPLOAD test for all clients..."
  for i in "${!CLIENTS[@]}"; do
    client=${CLIENTS[$i]}
    port=$((BASE_PORT + i))
    ssh "$client" \
      "iperf3 -c $SERVER_IP -p $port --reverse -P $PARALLEL_STREAMS -t $DURATION" \
      >"/tmp/iperf3_result_${client}_upload.log" 2>&1 &
    client_pids+=($!)
  done
  for pid in "${client_pids[@]}"; do wait "$pid"; done
  echo "[✓] Upload phase complete."

  client_pids=()
  echo "[*] Starting DUPLEX test for all clients..."
  for i in "${!CLIENTS[@]}"; do
    client=${CLIENTS[$i]}
    port=$((BASE_PORT + i))
    ssh "$client" \
      "iperf3 -c $SERVER_IP -p $port --bidir -P $PARALLEL_STREAMS -t $DURATION" \
      >"/tmp/iperf3_result_${client}_duplex.log" 2>&1 &
    client_pids+=($!)
  done
  for pid in "${client_pids[@]}"; do wait "$pid"; done
  echo "[✓] Duplex phase complete."
}

show_summary() {
  echo -e "\n===================== SUMMARY ================================"
  printf "%-16s %-6s %-12s %-12s %-12s\n" "Client" "Port" "Download" "Upload" "Duplex"
  echo "---------------------------------------------------------------"

  for i in "${!CLIENTS[@]}"; do
    c=${CLIENTS[$i]}
    p=$((BASE_PORT + i))

    d_rate=$(grep '\[SUM\]' "/tmp/iperf3_result_${c}_download.log" \
               | grep -Eo '[0-9.]+ Gbits/sec' | tail -1 | awk '{print $1}')
    [[ -z "$d_rate" ]] && d_rate=$(grep -Eo '[0-9.]+ Gbits/sec' "/tmp/iperf3_result_${c}_download.log" | tail -1 | awk '{print $1}')

    u_rate=$(grep '\[SUM\]' "/tmp/iperf3_result_${c}_upload.log" \
               | grep -Eo '[0-9.]+ Gbits/sec' | tail -1 | awk '{print $1}')
    [[ -z "$u_rate" ]] && u_rate=$(grep -Eo '[0-9.]+ Gbits/sec' "/tmp/iperf3_result_${c}_upload.log" | tail -1 | awk '{print $1}')

    x_rate=$(grep '\[SUM\]' "/tmp/iperf3_result_${c}_duplex.log" \
               | grep -Eo '[0-9.]+ Gbits/sec' | tail -1 | awk '{print $1}')
    [[ -z "$x_rate" ]] && x_rate=$(grep -Eo '[0-9.]+ Gbits/sec' "/tmp/iperf3_result_${c}_duplex.log" | tail -1 | awk '{print $1}')

    printf "%-16s %-6s %-12s %-12s %-12s\n" "$c" "$p" "${d_rate:-N/A}" "${u_rate:-N/A}" "${x_rate:-N/A}"
  done
  echo "==============================================================="
}

cleanup_servers() {
  echo "[*] Cleaning up iperf3 server processes..."
  pkill -f "iperf3 -s" || true
}

###############################################################################
# Main flow
###############################################################################
echo "[*] Starting benchmark..."
validate_server_ip
check_connectivity

# Install iperf3 everywhere (silent if already present)
for c in "${CLIENTS[@]}"; do ensure_iperf3_installed "$c"; done
ensure_iperf3_installed localhost

start_iperf3_servers
start_clients
show_summary
cleanup_servers

