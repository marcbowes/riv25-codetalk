#!/bin/bash

set -e

# Parse arguments
THREADS=1
ENDPOINT=""
while [[ $# -gt 0 ]]; do
    case $1 in
        --threads)
            THREADS="$2"
            shift 2
            ;;
        --endpoint)
            ENDPOINT="$2"
            shift 2
            ;;
        *)
            echo "Unknown argument: $1"
            echo "Usage: $0 --endpoint <endpoint> [--threads N]"
            exit 1
            ;;
    esac
done

if [ -z "$ENDPOINT" ]; then
    echo "Usage: $0 --endpoint <endpoint> [--threads N]"
    exit 1
fi

export PGDATABASE=postgres
export PGUSER=admin
export PGPASSWORD=$(aws dsql generate-db-connect-admin-auth-token --hostname $ENDPOINT --region us-west-2)
export PGSSLMODE=require

echo "Setting up database..."
psql -h $ENDPOINT <<EOF
DROP TABLE IF EXISTS accounts2;
CREATE TABLE accounts2 (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    balance DECIMAL NOT NULL
);
EOF

# Cleanup function for ctrl-c
cleanup() {
    echo ""
    echo "Interrupted! Stopping workers..."
    for pid in "${pids[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null
        fi
    done
    wait
    echo "Cleaning up worker files..."
    rm -f uuids_worker_*.txt
    exit 1
}

trap cleanup SIGINT SIGTERM

# Clear main output file
> uuids.txt

# Total transactions to run
TOTAL_TRANSACTIONS=1000
TRANSACTIONS_PER_THREAD=$((TOTAL_TRANSACTIONS / THREADS))
REMAINDER=$((TOTAL_TRANSACTIONS % THREADS))

echo "Running $TOTAL_TRANSACTIONS transactions across $THREADS thread(s)..."

# Worker function
worker() {
    local worker_id=$1
    local start=$2
    local end=$3
    local output_file="uuids_worker_${worker_id}.txt"

    > "$output_file"

    for ((i=start; i<=end; i++)); do
        psql -h $ENDPOINT -t -A -q <<EOF >> "$output_file"
INSERT INTO accounts2 (balance)
SELECT 100
FROM generate_series(1, 1000)
RETURNING id;
EOF
        echo "[Worker $worker_id] Transaction $i/$TOTAL_TRANSACTIONS completed"
    done
}

# Launch worker threads
pids=()
start=1
for ((t=1; t<=THREADS; t++)); do
    if [ $t -eq $THREADS ]; then
        end=$((start + TRANSACTIONS_PER_THREAD - 1 + REMAINDER))
    else
        end=$((start + TRANSACTIONS_PER_THREAD - 1))
    fi

    worker $t $start $end &
    pids+=($!)

    start=$((end + 1))
done

# Wait for all workers to complete
for pid in "${pids[@]}"; do
    wait $pid || {
        echo "Worker failed!"
        cleanup
    }
done

# Concatenate worker files into final output
echo "Merging worker outputs..."
for ((t=1; t<=THREADS; t++)); do
    if [ -f "uuids_worker_${t}.txt" ]; then
        cat "uuids_worker_${t}.txt" >> uuids.txt
        rm "uuids_worker_${t}.txt"
    fi
done

echo "Setup complete! Generated $(wc -l < uuids.txt) UUIDs in uuids.txt"
