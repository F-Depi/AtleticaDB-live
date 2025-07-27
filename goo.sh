#!/bin/bash

run_script() {
    year=$1
    while true; do
        echo "Starting year $year"
        python3 src/link_risultati.py "$year"
        status=$?
        if [ $status -eq 0 ]; then
            echo "Year $year completed successfully."
            break
        else
            echo "Year $year failed. Restarting in 2s..."
            sleep 2
        fi
    done
}

for year in 2017 2018 2019 2020 2021 2022 2023; do
    run_script "$year" &
done

wait

