# FFMPEG runner for NodeJS and GCS

### Configuring boot disk template
```bash
sudo add-apt-repository ppa:jonathonf/ffmpeg-3 # Enter to confirm
sudo apt-get update
sudo apt-get -y upgrade
sudo apt install -y ffmpeg libav-tools x264 x265

sudo curl -sL https://deb.nodesource.com/setup_6.x -o nodesource_setup.sh
sudo bash nodesource_setup.sh
sudo apt-get install nodejs -y
sudo npm install pm2 -g

# Route 8080 to port 80 to allow node to access port 80 without root privileges
sudo iptables -t nat -I PREROUTING -p tcp --dport 80 -j REDIRECT --to-port 8080

# Create ROOT RSA private key then copy to Github repo (pull access only)
sudo ssh-keygen -t rsa
sudo cat /root/.ssh/id_rsa.pub
```


### Startup script for each new instance
```bash
#! /bin/bash

ssh-keyscan github.com >> ~/.ssh/known_hosts
git clone git@github.com:xyk2/broadcast.cx-ffmpeg-runner.git

cd /broadcast.cx-ffmpeg-runner
npm install
PM2_HOME=/root/.pm2 pm2 kill
PM2_HOME=/root/.pm2 NODE_ENV=production pm2 start app.js
```


### Shutdown script before preemption
* Call localhost endpoint to reset running job in DB queue
* Terminate PM2
*

### Workflow to upload & transcode & tag game videos from YouTube
* `youtube-dl` to download and merge original files
* `gsutil -m cp` to `broadcast-cx-raw-recordings` bucket
* Insert filename, size, and format into `assets_game_footage` table
* Insert type, id, status into `assets_transcode_queue`
* Find game ID of game film, insert into `assets_game_footage` which will be populated
* Find start_record_time of game film, insert into `assets_game_footage`


### Useful snippets
`youtube-dl`
youtube-dl --external-downloader aria2c --external-downloader-args '-x 8' https://www.youtube.com/playlist?list=PLUz3zvlwsLgWJRmJo8fLOTMiXkFC25S61 --playlist-start 46 --playlist-end 150


### ffmpeg errors and possible errors
* `ffmpeg exited with code 1: Conversion failed!` -max_muxing_queue_size 1024





