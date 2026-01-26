# set -x

############### Validations ###############

# Check if /usr/bin/time is installed
if ! command -v $TIME_CMD &> /dev/null; then
    echo "Error: /usr/bin/time is not installed. Please install it to run this script."
    exit 1
fi

# EXTRA_REQUESTER_NODES cannot be set and should be empty if IS_MULTINODE is false
if [[ "$IS_MULTINODE" == false && ${#EXTRA_REQUESTER_NODES[@]} -ne 0 ]]; then
    echo "Error: EXTRA_REQUESTER_NODES cannot be set when IS_MULTINODE is false. Please remove it."
    exit 1
fi

# Validation for cgroupv2 variables
if [[ "$CGROUPV2_ENABLED" == true ]]; then
    if [ -z "$CGROUPV2_CGROUP_NAME" ]; then
        echo "Error: CGROUPV2_CGROUP_NAME is not set. Exiting."
        exit 1
    fi
    if [ -z "$CGROUPV2_RBPS" ]; then
        echo "Error: CGROUPV2_RBPS is not set. Exiting."
        exit 1
    fi
    if [ -z "$CGROUPV2_WBPS" ]; then
        echo "Error: CGROUPV2_WBPS is not set. Exiting."
        exit 1
    fi
    if [ -z "$CGROUPV2_RIOPS" ]; then
        echo "Error: CGROUPV2_RIOPS is not set. Exiting."
        exit 1
    fi
    if [ -z "$CGROUPV2_WIOPS" ]; then
        echo "Error: CGROUPV2_WIOPS is not set. Exiting."
        exit 1
    fi
    if [ -z "$CGROUPV2_DATA_FOLDER" ]; then
        echo "Error: CGROUPV2_DATA_FOLDER is not set. Exiting."
        exit 1
    fi
fi

check_command_installed() {
    local is_remote="$1"
    local cmd_name="$2"
    if [ "$is_remote" = "true" ]; then
        run_on_nodes "command -v $cmd_name >/dev/null 2>&1" || {
            echo "Error: '$cmd_name' command not found on one or more nodes. Please install '$cmd_name' package." >&2
            return 1
        }
    else
        if ! command -v "$cmd_name" >/dev/null 2>&1; then
            echo "Error: '$cmd_name' command not found. Please install '$cmd_name' package." >&2
            return 1
        fi
    fi
}

ycsb_python_check() {
    local err_str_no_python="Error: 'python' command not found on one or more extra requester nodes or current node. Please install 'python' package or use \"sudo update-alternatives --config python\" to point python to python2 for ycsb to run properly"

    check_command_installed "false" "python" # Ensure python is installed on the requester node, it is needed by ycsb
    # # Additionally check if the python points to python2
    if ! python --version 2>&1 | grep -q "Python 2"; then
        echo "Error: 'python' command does not point to Python 2. Please use \"sudo update-alternatives --config python\" to point python to python2 for ycsb to run properly" >&2
        return 1
    fi

    # Chcek if python is installed on all extra requester nodes
    if [[ -n "$EXTRA_REQUESTER_NODES" ]]; then
        # Check if "python" command is installed and points to python2 on all extra requester nodes
        for node in "${EXTRA_REQUESTER_NODES[@]}"; do
            if ! ssh -o BatchMode=yes -o ConnectTimeout=5  -o StrictHostKeyChecking=no "$node" "command -v python >/dev/null 2>&1"; then
            echo "$err_str_no_python" >&2
            return 1
            fi
            if ! ssh -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=no "$node" "python --version 2>&1 | grep -q 'Python 2'"; then
            echo "Error: 'python' command does not point to Python 2 on node $node. Please use \"sudo update-alternatives --config python\" to point python to python2 for ycsb to run properly" >&2
            return 1
            fi
        done
        # echo "✅ 'python' command is installed on all extra requester nodes: $EXTRA_REQUESTER_NODES."
    fi

    # echo "✅ 'python' command is installed on all nodes."
}

multinode_validations() {
    # Check if ansible is installed
    if ! command -v ansible &> /dev/null; then
        echo "Error: ansible is not installed. Please install ansible to run this script."
        exit 1
    fi

    # Check if java is installed
    if ! command -v java &> /dev/null; then
        echo "Error: java is not installed. Please install java to run this script."
        exit 1
    fi

    # Check if NODE_LIST contains any duplicate IPs
    declare -A seen_ips
    for node in "${NODE_LIST[@]}"; do
        if [[ -n "${seen_ips[$node]}" ]]; then
            echo "Error: Duplicate IP found in NODE_LIST: $node. Please remove duplicates."
            echo "NODE_LIST: ${NODE_LIST[*]}"
            echo "Exiting..."
            exit 1
        fi
        seen_ips["$node"]=1
    done

    # If EXTRA_REQUESTER_NODES is set and non-empty, then do the following checks
    if [[ -n "$EXTRA_REQUESTER_NODES" ]]; then
        # Check if EXTRA_REQUESTER_NODES contains any duplicate IPs
        declare -A seen_extra_requester_ips
        for node in "${EXTRA_REQUESTER_NODES[@]}"; do
            if [[ -n "${seen_extra_requester_ips[$node]}" ]]; then
                echo "Error: Duplicate IP found in EXTRA_REQUESTER_NODES: $node. Please remove duplicates."
                echo "EXTRA_REQUESTER_NODES: ${EXTRA_REQUESTER_NODES[*]}"
                echo "Exiting..."
                exit 1
            fi
            seen_extra_requester_ips["$node"]=1
        done

        # Check if all nodes in EXTRA_REQUESTER_NODES are reachable via SSH using ansible
        echo "Checking if all nodes in EXTRA_REQUESTER_NODES are reachable via SSH..."
        for node in "${EXTRA_REQUESTER_NODES[@]}"; do
            if ! ansible all -i "$node," -m shell -a hostname -u "$USER" --timeout=5 >/dev/null 2>&1; then
                echo "Error: Node $node in EXTRA_REQUESTER_NODES is unreachable via SSH." >&2
                exit 1
            fi
        done

        # Check if the requester node IP is reachable via SSH from all nodes in EXTRA_REQUESTER_NODES
        echo "Checking if REQUESTER_NODE_IP ($REQUESTER_NODE_IP) is reachable via SSH from all nodes in EXTRA_REQUESTER_NODES..."
        for node in "${EXTRA_REQUESTER_NODES[@]}"; do
            if ! ssh -o BatchMode=yes -o ConnectTimeout=5 "$node" "ssh -o BatchMode=yes -o ConnectTimeout=5 $REQUESTER_NODE_IP hostname" >/dev/null 2>&1; then
                echo "Error: REQUESTER_NODE_IP ($REQUESTER_NODE_IP) is unreachable via SSH from node $node."
                exit 1
            fi
        done

        # Check if current node is in EXTRA_REQUESTER_NODES
        CURRENT_IPS=($(hostname -I))
        CURRENT_HOSTNAME=$(hostname)
        for node in "${EXTRA_REQUESTER_NODES[@]}"; do
            for ip in "${CURRENT_IPS[@]}"; do
                if [[ "$node" == "$ip" ]]; then
                    echo "Error: Current node IP ($ip) is present in EXTRA_REQUESTER_NODES. Please remove it."
                    exit 1
                fi
            done
        done

        # Check if any nodes from NODE_LIST are present in EXTRA_REQUESTER_NODES
        for node in "${NODE_LIST[@]}"; do
            for extra_node in "${EXTRA_REQUESTER_NODES[@]}"; do
                if [[ "$node" == "$extra_node" ]]; then
                    echo "Error: Node $node from NODE_LIST is present in EXTRA_REQUESTER_NODES. Please remove it."
                    exit 1
                fi
            done
        done


    fi

    # Check if NODE_LIST is not empty
    if [[ ${#NODE_LIST[@]} -eq 0 ]]; then
        echo "Error: NODE_LIST must be specified for multinode mode."
        exit 1
    fi

    # Check if REQUESTER_NODE_IP belongs to the current node
    CURRENT_IPS=($(hostname -I))
    CURRENT_HOSTNAME=$(hostname)

    FOUND_REQUESTER_IP=false
    for ip in "${CURRENT_IPS[@]}"; do
        if [[ "$ip" == "$REQUESTER_NODE_IP" ]]; then
            FOUND_REQUESTER_IP=true
            break
        fi
    done

    if [[ "$FOUND_REQUESTER_IP" != true ]]; then
        echo "Error: REQUESTER_NODE_IP ($REQUESTER_NODE_IP) does not belong to this node (IPs: ${CURRENT_IPS[*]})."
        exit 1
    fi

    # Check if current node is in NODE_LIST. We don't want to run UCSB on the requester node.
    CURRENT_IPS=($(hostname -I))
    CURRENT_HOSTNAME=$(hostname)
    for node in "${NODE_LIST[@]}"; do
        for ip in "${CURRENT_IPS[@]}"; do
            if [[ "$node" == "$ip" ]]; then
                echo "Error: Current node IP ($ip) is present in NODE_LIST. Please remove it. We don't want to run DB on the requester node."
                exit 1
            fi
        done
        if [[ "$node" == "$CURRENT_HOSTNAME" ]]; then
            echo "Error: Current node hostname ($CURRENT_HOSTNAME) is present in NODE_LIST. Please remove it. We don't want to run DB on the requester node."
            exit 1
        fi
    done

    # Check if all nodes are reachable using ansible (dummy command: hostname)
    hosts_csv=$(IFS=,; echo "${NODE_LIST[*]}")
    if ! ansible all -i "$hosts_csv," -m shell -a hostname -u "$USER" --timeout=5 >/dev/null 2>&1; then
        echo "Error: one or more nodes in NODE_LIST are unreachable via SSH." >&2
        for node in "${NODE_LIST[@]}"; do
            if ! ssh -o BatchMode=yes -o ConnectTimeout=5 "$node" hostname >/dev/null 2>&1; then
                echo "  - $node is unreachable via SSH."
            fi
        done
        exit 1
    fi

    # Check if REQUESTER_NODE_IP is reachable via SSH from all nodes in NODE_LIST
    echo "Checking if REQUESTER_NODE_IP ($REQUESTER_NODE_IP) is reachable via SSH from all nodes in NODE_LIST..."
    for node in "${NODE_LIST[@]}"; do
        if ! ssh -o BatchMode=yes -o ConnectTimeout=5 "$node" "ssh -o BatchMode=yes -o ConnectTimeout=5 $REQUESTER_NODE_IP hostname" >/dev/null 2>&1; then
            echo "Error: REQUESTER_NODE_IP ($REQUESTER_NODE_IP) is unreachable via SSH from node $node."
            exit 1
        fi
    done

    echo "✅ All nodes in NODE_LIST are reachable via SSH."
}


###############################################################################
# validate_cassandra_version <cassandra_home> <db_type>
#
# cassandra_home : path to CASSANDRA_HOME_DIR
# db_type        : cassandra3 | cassandra4  (expects major version matching)
#
# Validates that:
#   - cassandra binary exists
#   - cassandra binary prints a version
#   - version matches the expected major version family
###############################################################################
###############################################################################
# validate_cassandra_version <cassandra_home> <db_type>
#
# cassandra_home : path to CASSANDRA_HOME_DIR
# db_type        : cassandra3 | cassandra4 | cassandra5
#
# Validates that:
#   - cassandra binary exists
#   - cassandra binary prints a version
#   - version matches the expected major version family
###############################################################################
validate_cassandra_version() {
    local cass_home="$1"
    local db_type="$2"

    # Check binary existence
    local cass_bin="$cass_home/bin/cassandra"
    if [[ ! -x "$cass_bin" ]]; then
        echo "❌ Cassandra binary not found or not executable: $cass_bin"
        return 1
    fi

    # Extract version
    local version
    echo "for version using $cass_bin"
    version="$("$cass_bin" -v 2>/dev/null)"
    echo "$version"
    if [[ -z "$version" ]]; then
        echo "❌ Failed to read Cassandra version from: $cass_bin"
        return 1
    fi

    # Normalize whitespace
    version="$(echo "$version" | tr -d '[:space:]')"

    # Validate based on DB_TYPE
    case "$db_type" in
        cassandra3)
            if [[ "$version" =~ ^3\.[0-9]+\.[0-9]+$ ]]; then
                echo "✅ Cassandra 3.x verified: $version"
            else
                echo "❌ Expected Cassandra 3.x but found: $version"
                return 1
            fi
            ;;
        cassandra4)
            if [[ "$version" =~ ^4\.[0-9]+\.[0-9]+$ ]]; then
                echo "✅ Cassandra 4.x verified: $version"
            else
                echo "❌ Expected Cassandra 4.x but found: $version"
                return 1
            fi
            ;;
        cassandra5)
            if [[ "$version" =~ ^5\.[0-9]+\.[0-9]+$ ]]; then
                echo "✅ Cassandra 5.x verified: $version"
            else
                echo "❌ Expected Cassandra 5.x but found: $version"
                return 1
            fi
            ;;
        *)
            echo "❌ Unknown DB_TYPE: $db_type (expected cassandra3, cassandra4, or cassandra5)"
            return 1
            ;;
    esac

    return 0
}

