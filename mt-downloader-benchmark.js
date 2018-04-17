var url = require("url")
var path = require("path")
var mtd = require('zeltice-mt-downloader')

var target_url = "http://storage.googleapis.com/broadcast-cx-raw-recordings/8dc9_F70D11E4-8ADE-46B4-BE4A-4A791CA51166.MOV"
var file_name = path.basename(url.parse(target_url).pathname)
var file_path = path.join(__dirname, "8dc9_F70D11E4-8ADE-46B4-BE4A-4A791CA51166.MOV")

var start_time = null;
var mt_downloading = null;

var downloader = new mtd(file_path, target_url, {
  count: 8, // (Default: 2)
  method: 'GET', // (Default: GET)
  port: 80, // (Default: 80)
  timeout: 5, // (Default: 5 seconds)
  onStart: function(meta) {
    console.log('Download started mt-downloader...');
    start_time = Date.now();

    mt_downloading = true;

    setInterval(function() {
      if(mt_downloading) {
        for(var x in meta.threads) {
          console.log(meta.threads[x].start / 1000, meta.threads[x].end / 1000, meta.threads[x].position / 1000);
        }
      }
    }, 2000);
  },

  //Triggered when the download is completed
  onEnd: function(err, result) {
    if (err) console.error(err);
    else console.log('Download complete mt-downloader.');
    mt_downloading = false;
    console.log((Date.now() - start_time)/1000);


    var google_cloud = require('google-cloud')({
      projectId: 'broadcast-cx',
      keyFilename: 'broadcast-cx-bda0296621a4.json'
    });

    var gcs = google_cloud.storage();
    var bucket = gcs.bucket('broadcast-cx-raw-recordings');

    start_time = Date.now();
    console.log('Download started google-cloud...');

    bucket.file('8dc9_F70D11E4-8ADE-46B4-BE4A-4A791CA51166.MOV').download({
      destination: 'GCP_8dc9_F70D11E4-8ADE-46B4-BE4A-4A791CA51166.MOV'
    }, function(err) {

      console.log('Download complete google-cloud.');
      console.log((Date.now() - start_time)/1000);
    });
  }
});

downloader.start();