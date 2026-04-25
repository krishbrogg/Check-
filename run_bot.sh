#!/bin/bash
cd "$(dirname "$0")"
while true; do
    python main.py
    echo "Bot crashed, restarting in 3 seconds..."
    sleep 3
done
