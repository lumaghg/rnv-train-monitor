# RNV Train Monitor

RNV train monitor is an application that uses the GTFS and GTFS-RT API from the rnv transit company to display the live positions of all vehicles in the area of Heidelberg on a 64*32 LED Matrix.
This project is still in progress and therefore still lacks documentation.

# rnv-train-monitor
Steps:  
- [X] Auslesen der Fahrplandaten für die Linie 22 aus der API
- [X] Digitales Darstellen der Fahrplandaten der Linie 22
- [X] Software für die Anzeige der Linie 22 auf der Hardware
- [X] Auslesen der Echtzeitdaten der Linie 22 aus der API
- [ ] Inkscape für Bauteile der Linie 22
- [ ] Manufacturing der Linie 22



- [ ] Erweiterung auf das gesamte Streckennetz (1 LED pro Streckenabschnitt)
- [ ] Erweiterung auf 2 LEDs pro Streckenabschnitt oder 1 LED pro Linie


# Installation

virtuelles environment myenv in rnv-train-monitor anlegen
Dann 
1. neuer Ordner rpi-rgb-led-matrix
2. Inhalt von https://github.com/hzeller/rpi-rgb-led-matrix/tree/master reinkopieren
3. Im Ordner rpi-rgb-led-matrix:
```sh
sudo apt-get update && sudo apt-get install python3-dev cython3 -y
make build-python 
sudo make install-python 
```

und

.env file mit secrets anlegen


# Execution

execute preprocessing script every day at midnight 

execute extract active vehicles script every 10 seconds (see convenience script)
execute display_csv script once on startup (runs in loop)

example:
* * * * * sudo bash /home/robin/Documents/github/rnv-train-monitor/src/extract_active_vehicles_loopwrapper.bash >> /home/robin/cronlogs/crontab_eav.log 2>&1
0 3 * * * sudo bash /home/robin/Documents/github/rnv-train-monitor/src/preprocess_static_wrapper.bash >> /home/robin/cronlogs/crontab_ps.log 2>&1
@reboot sleep 10;sudo bash /home/robin/Documents/github/rnv-train-monitor/src/display_csv_wrapper.bash >> /home/robin/cronlogs/crontab_dcsv.log 2>&1
@reboot sleep 20;sudo bash /home/robin/Documents/github/rnv-train-monitor/src/preprocess_static_wrapper.bash >> /home/robin/cronlogs/crontab_ps.log 2>&1
