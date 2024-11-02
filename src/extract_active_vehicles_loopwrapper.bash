#!/bin/bash
source /home/robin/Documents/github/rnv-train-monitor/myenv/bin/activate

cd /home/robin/Documents/github/rnv-train-monitor/src

while true
do
    # Invoke your command here, the same as you would from your crontab
    sudo python ./extract_active_vehicles.py
done