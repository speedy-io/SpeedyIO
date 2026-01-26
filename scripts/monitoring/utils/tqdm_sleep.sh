# Function to simulate a tqdm-like sleep
tqdm_sleep() {
    local duration=$1
    local interval=1  # Update progress every 1 second
    local total_ticks=$((duration / interval))
    local progress=0

    echo "Sleeping for $duration seconds..."

    echo -n "["
    while [ $progress -lt $total_ticks ]; do
        echo -n "="
        sleep $interval
        progress=$((progress + 1))
    done
    echo "]"
    echo "Sleep complete!"
}