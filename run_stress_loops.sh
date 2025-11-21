#!/bin/bash

# === Configuration ===
# Path to your compiled stress test binary
# Make sure to compile in release mode first: cargo build --release --bin stress_test
BINARY_PATH="./target/release/stress_test"

# --- Test Parameters ---
# These are the default arguments from your stress_test.rs file
# Change them to match your test image and server config
IMAGE_FILE="my_image.png"
SERVER_CONFIG="servers.conf"

# --- Loop Configuration ---
START_REQUESTS=100
REQUEST_INCREMENT=100

START_THREADS=10
THREAD_INCREMENT=10

# Total number of incremental runs you want to perform
# For example, 10 steps will go from (100r, 10t) up to (1000r, 100t)
NUM_STEPS=10
# === End Configuration ===


echo "Starting stress test escalation script..."
echo "Will perform $NUM_STEPS test runs."
echo ""

# Check if the binary exists and is executable
if [ ! -x "$BINARY_PATH" ]; then
    echo "Error: Binary not found or not executable at $BINARY_PATH"
    echo "Please build the project first using:"
    echo "cargo build --release --bin stress_test"
    exit 1
fi

# Initialize current values
current_requests=$START_REQUESTS
current_threads=$START_THREADS

# Loop from 1 up to NUM_STEPS
for (( i=1; i<=$NUM_STEPS; i++ ))
do
    echo "================================================="
    echo "RUN $i / $NUM_STEPS"
    echo "Running with: $current_requests requests, $current_threads threads"
    echo "================================================="
    
    # Construct and run the command
    # The output of your Rust program will be printed directly to the console
    "$BINARY_PATH" -n "$current_requests" -t "$current_threads" -i "$IMAGE_FILE" -s "$SERVER_CONFIG"
    
    echo ""
    echo "Run $i finished. Waiting 5 seconds before next run..."
    echo "================================================="
    echo ""
    
    # Increment values for the next loop
    current_requests=$((current_requests + REQUEST_INCREMENT))
    current_threads=$((current_threads + THREAD_INCREMENT))
    
    # Add a short delay between tests so you can read the output
    # or to let the server recover, if needed.
    # Remove this if you want runs to be back-to-back.
    if [ $i -lt $NUM_STEPS ]; then
        sleep 5
    fi
done

echo "All stress test runs are complete."
