#!/bin/bash

while true; do
    echo "Running script..."
    python src/link_risultati.py
    status=$?

    if [ $status -eq 0 ]; then
        echo "Script completed successfully."
        break
    else
        echo "Script failed with status $status. Restarting..."
        sleep 1
    fi
done

