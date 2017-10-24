# FFMPEG runner for NodeJS and GCS

### Configuring boot disk template

```bash
sudo add-apt-repository ppa:jonathonf/ffmpeg-3
sudo apt-get update
sudo apt-get -y upgrade
sudo apt install -y ffmpeg libav-tools x264 x265

curl -sL https://deb.nodesource.com/setup_6.x -o nodesource_setup.sh
sudo bash nodesource_setup.sh
sudo apt-get install nodejs -y

sudo npm install pm2 -g

# Route 8080 to port 80 to allow node to access port 80 without root privileges
sudo iptables -t nat -I PREROUTING -p tcp --dport 80 -j REDIRECT --to-port 8080


# Create RSA private key then copy to Github repo (pull access only)
ssh-keygen -t rsa
cat /home/kaoxiaoyang/.ssh/id_rsa.pub

```


### Startup script for each instance
```bash
ssh-keyscan github.com >> ~/.ssh/known_hosts # Add github addresses to known SSH hosts
git clone git@github.com:xyk2/broadcast.cx-ffmpeg-runner.git

cd ~/broadcast.cx-ffmpeg-runner
npm install
NODE_ENV=production pm2 start app.js
```




