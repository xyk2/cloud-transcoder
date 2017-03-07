#!/bin/bash

sudo apt-get update
curl -sL https://deb.nodesource.com/setup_6.x | sudo -E bash -
sudo apt-get install -y nodejs
sudo npm install npm -g
sudo npm install pm2 -g

sudo apt-get install -y ffmpeg





#cd /usr/local/WowzaStreamingEngine/nodejs
#sudo npm install
#sudo pm2 start /usr/local/WowzaStreamingEngine/nodejs/app.js
#sudo pm2 save
#sudo pm2 startup
