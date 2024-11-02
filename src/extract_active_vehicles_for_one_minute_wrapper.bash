#!/bin/bash
source /home/robin/Documents/github/rnv-train-monitor/myenv/bin/activate

cd /home/robin/Documents/github/rnv-train-monitor/src

for (( i=1; i<=6; i++ )); do
    # Invoke your command here, the same as you would from your crontab
    # The & at the end will run the job in a background shell, so the 30 second sleep will start immediately 
    sudo python ./extract_active_vehicles.py &
    sleep 10
done