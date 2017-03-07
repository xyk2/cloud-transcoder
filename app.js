var fs = require('fs');
var restify = require('restify');
var ffmpeg = require('fluent-ffmpeg');
const WebSocket = require('ws');
var dir = require('node-dir');
var path = require('path');


var server = restify.createServer({
	name: 'ffmpeg-runner'
});

const wss = new WebSocket.Server({ port: 8081 });

wss.broadcast = function broadcast(data) {
	wss.clients.forEach(function each(client) {
		if(client.readyState === WebSocket.OPEN) {
			client.send(data);
		}
	});
};

var google_cloud = require('google-cloud')({
	projectId: 'broadcast-cx',
	keyFilename: 'keys/broadcast-cx-bda0296621a4.json'
});

var gcs = google_cloud.storage();
var bucket = gcs.bucket('broadcast-cx-raw-recordings');
var dest_bucket = gcs.bucket('broadcast-cx-sandbox');

//ffmpeg.setFfmpegPath('/Users/XYK/Desktop/ffmpeg'); // Explicitly set ffmpeg and ffprobe paths
//ffmpeg.setFfprobePath('/Users/XYK/Desktop/ffprobe');

server.get('/transcode/HLS/:filename', hlsTranscode);
server.get('/upload/', upload);


function hlsTranscode(req, res, next) {
	if(!req.params.filename) return res.send(400, {status: 1, message: "Invalid Request."});

	_transcodedRenditionsCount = 0;

	HD_720P_TRANSCODE = function(filename, callback) { 
		ffmpeg(filename, { presets: '/usr/local/ffmpeg-runner/broadcast.cx-ffmpeg-runner/presets' }).preset('hls')
		.videoBitrate(3000)
		.audioBitrate('96k')
		.size('1280x720')
		.saveToFile('/usr/local/ffmpeg-runner/broadcast.cx-ffmpeg-runner/_outputs/720p_3000k.m3u8')
		.on('start', function(commandLine) {
			console.log('FFMPEG: ' + commandLine);
			wss.broadcast('FFMPEG: ' + commandLine);
		})
		.on('progress', function(progress) {
			console.log('Processing: ' + JSON.stringify(progress) + ' frames');
			wss.broadcast(JSON.stringify(progress));
		})
		.on('stderr', function(stderrLine) {
		    //console.log('Stderr output: ' + stderrLine);
		})
		.on('error', function(err, stdout, stderr) {
		    console.log('Cannot process video: ' + err.message);
		    wss.broadcast("Cannot process video: " + err.message);
		})
		.on('end', function(stdout, stderr) {
		    console.log('Transcoding succeeded!');
		    wss.broadcast('Transcoding successful.');
		    _transcodedRenditionsCount++;
		    callback();
		});
	}

	SD_480P_TRANSCODE = function(filename, callback) {
		ffmpeg(filename, { presets: '/usr/local/ffmpeg-runner/broadcast.cx-ffmpeg-runner/presets' }).preset('hls')
		.videoBitrate(1500)
		.audioBitrate('96k')
		.size('854x480')
		.saveToFile('/usr/local/ffmpeg-runner/broadcast.cx-ffmpeg-runner/_outputs/480p_1500k.m3u8')
		.on('start', function(commandLine) {
			console.log('FFMPEG: ' + commandLine);
			wss.broadcast('FFMPEG: ' + commandLine);
		})
		.on('progress', function(progress) {
			console.log('Processing: ' + JSON.stringify(progress) + ' frames');
			wss.broadcast(JSON.stringify(progress));
		})
		.on('stderr', function(stderrLine) {
		    //console.log('Stderr output: ' + stderrLine);
		})
		.on('error', function(err, stdout, stderr) {
		    console.log('Cannot process video: ' + err.message);
		    wss.broadcast("Cannot process video: " + err.message);
		})
		.on('end', function(stdout, stderr) {
		    console.log('Transcoding succeeded!');
		    wss.broadcast('Transcoding successful.');
		    _transcodedRenditionsCount++;
		    callback();
		});
	}

	SD_360P_TRANSCODE = function(filename, callback) {
		ffmpeg(filename, { presets: '/usr/local/ffmpeg-runner/broadcast.cx-ffmpeg-runner/presets' }).preset('hls')
		.videoBitrate(850)
		.audioBitrate('96k')
		.size('640x360')
		.saveToFile('/usr/local/ffmpeg-runner/broadcast.cx-ffmpeg-runner/_outputs/360p_850k.m3u8')
		.on('start', function(commandLine) {
			console.log('FFMPEG: ' + commandLine);
			wss.broadcast('FFMPEG: ' + commandLine);
		})
		.on('progress', function(progress) {
			console.log('Processing: ' + JSON.stringify(progress) + ' frames');
			wss.broadcast(JSON.stringify(progress));
		})
		.on('stderr', function(stderrLine) {
		    //console.log('Stderr output: ' + stderrLine);
		})
		.on('error', function(err, stdout, stderr) {
		    console.log('Cannot process video: ' + err.message);
		    wss.broadcast("Cannot process video: " + err.message);
		})
		.on('end', function(stdout, stderr) {
		    console.log('Transcoding succeeded!');
		    wss.broadcast('Transcoding successful.');
		    _transcodedRenditionsCount++;
		    callback();
		});
	}

	SD_240P_TRANSCODE = function(filename, callback) {
		ffmpeg(filename, { presets: '/usr/local/ffmpeg-runner/broadcast.cx-ffmpeg-runner/presets' }).preset('hls')
		.videoBitrate(400)
		.audioBitrate('96k')
		.size('352x240')
		.saveToFile('/usr/local/ffmpeg-runner/broadcast.cx-ffmpeg-runner/_outputs/240p_400k.m3u8')
		.on('start', function(commandLine) {
			console.log('FFMPEG: ' + commandLine);
			wss.broadcast('FFMPEG: ' + commandLine);
		})
		.on('progress', function(progress) {
			console.log('Processing: ' + JSON.stringify(progress) + ' frames');
			wss.broadcast(JSON.stringify(progress));
		})
		.on('stderr', function(stderrLine) {
		    //console.log('Stderr output: ' + stderrLine);
		})
		.on('error', function(err, stdout, stderr) {
		    console.log('Cannot process video: ' + err.message);
		    wss.broadcast("Cannot process video: " + err.message);
		})
		.on('end', function(stdout, stderr) {
		    console.log('Transcoding succeeded!');
		    wss.broadcast('Transcoding successful.');
		    _transcodedRenditionsCount++;
		    callback();
		});
	}

	CREATE_INDEX_M3U8 = function() {
		fs.createReadStream('index.m3u8').pipe(fs.createWriteStream('/usr/local/ffmpeg-runner/broadcast.cx-ffmpeg-runner/_outputs/index.m3u8'));
	}

	bucket.file(req.params.filename).download({
	  destination: req.params.filename
	}, function(err) {
		if(err) {
			console.log(err.code, err.message);
			res.send({status: 0, message: err.code + ' ' + err.message});
			return;
		} // Error handling (bucket file not found in GCS)

		res.send({status: 0, message: "Transcodes started"});

		CREATE_INDEX_M3U8();
		HD_720P_TRANSCODE(req.params.filename, uploadToGCS);
		SD_480P_TRANSCODE(req.params.filename, uploadToGCS);
		SD_360P_TRANSCODE(req.params.filename, uploadToGCS);
		SD_240P_TRANSCODE(req.params.filename, uploadToGCS);
	});

	// Will not execute until all 4

	function uploadToGCS() {
		if(_transcodedRenditionsCount != 4) return;

		dir.files('/usr/local/ffmpeg-runner/broadcast.cx-ffmpeg-runner/_outputs', function(err, files) {
			if (err) throw err;
			files.forEach(function(file, index) {
				if(path.extname(file) === '.m3u8' || path.extname(file) === '.ts') { // Only upload M3U8s and transport streams

					var _options = { // GCS destination bucket folder and file paths
					  destination: path.basename(req.params.filename, '.mp4') + '/' + path.basename(file) // Directory of /filenamewithoutextension/file
					};

					dest_bucket.upload(file, _options, function(err, gFileObj) {
						if(err) { // Error handling
							console.log(err);
							return;
						}

						if(gFileObj.name.indexOf('.m3u8') != -1) {
							var metadata = {
								contentType: 'application/x-mpegURL'
							};
						} else {
							var metadata = {
								contentType: 'video/MP2T'
							};
						}

						gFileObj.setMetadata(metadata, function(err, apiResponse) {});
						console.log('Uploaded ' + gFileObj.name);
						wss.broadcast('Uploaded ' + gFileObj.name);
					});
				}
			});
		});




	}


}

function upload(req, res, next) {
	dir.files('/usr/local/ffmpeg-runner/broadcast.cx-ffmpeg-runner/_outputs', function(err, files) {
	    if (err) throw err;
	    files.forEach(function(file, index) {
	    	if(path.extname(file) === '.m3u8' || path.extname(file) === '.ts') { // Only upload M3U8s and transport streams
	    		dest_bucket.upload(file, function(err, file) {
	    			if(err) { // Error handling
	    				console.log(err);
	    				return;
	    			}

	    			if(file.name.indexOf('.m3u8') != -1) {
	    				var metadata = {
	    				  contentType: 'application/x-mpegURL'
	    				};
	    			} else {
	    				var metadata = {
	    				  contentType: 'video/MP2T'
	    				};
	    			}

	    			file.setMetadata(metadata, function(err, apiResponse) {});

	    			console.log("Successful upload");
	    			
	    		});
	    	}
	    });

	});
}




if(process.env.NODE_ENV === 'production') _port = 80;
else _port = 8080;


server.listen(_port, function() {
	console.log('%s listening at %s', server.name, server.url);
});



