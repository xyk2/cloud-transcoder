# FFMPEG runner for NodeJS and GCS

### Configuring boot disk template
* Create SSH key and add to repository deploy keys
* Install ffmpeg, node, npm, PM2 
* Clone git directory at `usr/local/ffmpeg-runner/broadcast.cx-ffmpeg-runner`
* Route 8080 to port 80 to allow node to access port 80 without root privileges (sudo iptables -t nat -I PREROUTING -p tcp --dport 80 -j REDIRECT --to-port 8080)




