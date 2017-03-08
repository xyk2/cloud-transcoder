var fs = require('fs-extra');
var restify = require('restify');
var ffmpeg = require('fluent-ffmpeg');
const WebSocket = require('ws');
var dir = require('node-dir');
var path = require('path');
var request = require('request');

console.log(process.env);

if(process.env.PWD == '/usr/local/ffmpeg-runner/broadcast.cx-ffmpeg-runner') { // If there is a response from metadata server, means it is running on GCS
	_OUTPUT_PATH = '/usr/local/ffmpeg-runner/broadcast.cx-ffmpeg-runner/_outputs';
	_PRESETS_PATH = '/usr/local/ffmpeg-runner/broadcast.cx-ffmpeg-runner/presets';
	_PORT = 80;
	_WS_PORT = 80;

} else { // Running local
	_OUTPUT_PATH = '/Users/XYK/Desktop/ffmpeg_outputs';
	_PRESETS_PATH = '/Users/XYK/Desktop/Dropbox/broadcast.cx-ffmpeg-runner/presets';
	ffmpeg.setFfmpegPath('/Users/XYK/Desktop/ffmpeg'); // Explicitly set ffmpeg and ffprobe paths
	ffmpeg.setFfprobePath('/Users/XYK/Desktop/ffprobe');
	_PORT = 8080;
	_WS_PORT = 8081;
}



var server = restify.createServer({
	name: 'ffmpeg-runner'
});

const wss = new WebSocket.Server({ port: _WS_PORT });

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


server.get('/transcode/hls/:filename', hlsTranscode);


function hlsTranscode(req, res, next) {
	if(!req.params.filename) {
		wss.broadcast(JSON.stringify({'event': 'error', 'message': 'Invalid request.'}));
		res.send(400, {status: 1, message: "Invalid Request."});
	}

	_transcodedRenditionsCount = 0;

	HD_720P_TRANSCODE = function(filename, callback) { 
		_HD_720P = ffmpeg(filename, { presets: _PRESETS_PATH }).preset('hls')
		.videoBitrate(3000)
		.audioBitrate('96k')
		.size('1280x720')
		.saveToFile(_OUTPUT_PATH + '/720p_3000k.m3u8')
		.on('start', function(commandLine) {
			_ret = {'event': 'start', 'rendition': '720P_3000K', 'command': commandLine};

		    console.log(JSON.stringify(_ret));
		    wss.broadcast(JSON.stringify(_ret));

		})
		.on('progress', function(progress) {			
			progress['event'] = 'progress';
			progress['rendition'] = '720P_3000K';

			console.log(JSON.stringify(progress));
			wss.broadcast(JSON.stringify(progress));
		})
		.on('stderr', function(stderrLine) {
		    console.log('Stderr output: ' + stderrLine);
		})
		.on('error', function(err, stdout, stderr) {
			_ret = {'event': 'error', 'message': err.message};
			wss.broadcast(JSON.stringify(_ret));
		})
		.on('end', function(stdout, stderr) {
			_ret = {'event': 'success', 'rendition': '720P_3000K'};

		    console.log(JSON.stringify(_ret));
		    wss.broadcast(JSON.stringify(_ret));

		    _transcodedRenditionsCount++;
		    callback();
		});
	}

	SD_480P_TRANSCODE = function(filename, callback) {
		_SD_480P = ffmpeg(filename, { presets: _PRESETS_PATH }).preset('hls')
		.videoBitrate(1500)
		.audioBitrate('96k')
		.size('854x480')
		.saveToFile(_OUTPUT_PATH + '/480p_1500k.m3u8')
		.on('start', function(commandLine) {
			_ret = {'event': 'start', 'rendition': '480P_1500K', 'command': commandLine};

		    console.log(JSON.stringify(_ret));
		    wss.broadcast(JSON.stringify(_ret));
		})
		.on('progress', function(progress) {			
			progress['event'] = 'progress';
			progress['rendition'] = '480P_1500K';

			console.log(JSON.stringify(progress));
			wss.broadcast(JSON.stringify(progress));
		})
		.on('stderr', function(stderrLine) {
		    //console.log('Stderr output: ' + stderrLine);
		})
		.on('error', function(err, stdout, stderr) {
			_ret = {'event': 'error', 'message': err.message};
			wss.broadcast(JSON.stringify(_ret));
		})
		.on('end', function(stdout, stderr) {
			_ret = {'event': 'success', 'rendition': '480P_1500K'};

		    console.log(JSON.stringify(_ret));
		    wss.broadcast(JSON.stringify(_ret));

		    _transcodedRenditionsCount++;
		    callback();
		});
	}

	SD_360P_TRANSCODE = function(filename, callback) {
		_SD_360P = ffmpeg(filename, { presets: _PRESETS_PATH }).preset('hls')
		.videoBitrate(850)
		.audioBitrate('96k')
		.size('640x360')
		.saveToFile(_OUTPUT_PATH + '/360p_850k.m3u8')
		.on('start', function(commandLine) {
			_ret = {'event': 'start', 'rendition': '360P_850K', 'command': commandLine};

		    console.log(JSON.stringify(_ret));
		    wss.broadcast(JSON.stringify(_ret));
		})
		.on('progress', function(progress) {			
			progress['event'] = 'progress';
			progress['rendition'] = '360P_850K';

			console.log(JSON.stringify(progress));
			wss.broadcast(JSON.stringify(progress));
		})
		.on('stderr', function(stderrLine) {
		    //console.log('Stderr output: ' + stderrLine);
		})
		.on('error', function(err, stdout, stderr) {
			_ret = {'event': 'error', 'message': err.message};
			wss.broadcast(JSON.stringify(_ret));
		})
		.on('end', function(stdout, stderr) {
			_ret = {'event': 'success', 'rendition': '360P_850K'};

		    console.log(JSON.stringify(_ret));
		    wss.broadcast(JSON.stringify(_ret));

		    _transcodedRenditionsCount++;
		    callback();
		});
	}

	SD_240P_TRANSCODE = function(filename, callback) {
		_SD_240P = ffmpeg(filename, { presets: _PRESETS_PATH }).preset('hls')
		.videoBitrate(400)
		.audioBitrate('96k')
		.size('352x240')
		.saveToFile(_OUTPUT_PATH + '/240p_400k.m3u8')
		.on('start', function(commandLine) {
			_ret = {'event': 'start', 'rendition': '240P_400K', 'command': commandLine};

		    console.log(JSON.stringify(_ret));
		    wss.broadcast(JSON.stringify(_ret));
		})
		.on('progress', function(progress) {			
			progress['event'] = 'progress';
			progress['rendition'] = '240P_400K';

			console.log(JSON.stringify(progress));
			wss.broadcast(JSON.stringify(progress));
		})
		.on('stderr', function(stderrLine) {
		    //console.log('Stderr output: ' + stderrLine);
		})
		.on('error', function(err, stdout, stderr) {
			_ret = {'event': 'error', 'message': err.message};
			wss.broadcast(JSON.stringify(_ret));
		})
		.on('end', function(stdout, stderr) {
			_ret = {'event': 'success', 'rendition': '240P_400K'};

		    console.log(JSON.stringify(_ret));
		    wss.broadcast(JSON.stringify(_ret));

		    _transcodedRenditionsCount++;
		    callback();
		});
	}

	CREATE_INDEX_M3U8 = function() {
		fs.createReadStream('index.m3u8').pipe(fs.createWriteStream(_OUTPUT_PATH + '/index.m3u8'));
	}

	bucket.file(req.params.filename).download({
	  destination: req.params.filename
	}, function(err) {
		if(err) {
			console.log(err.code, err.message);
			wss.broadcast(JSON.stringify({'event': 'error', 'message': err.code + ' ' + err.message}));
			res.send({status: 0, message: err.code + ' ' + err.message});
			return;
		} // Error handling (bucket file not found in GCS)


		fs.emptyDir(_OUTPUT_PATH, err => {
			if (err) return console.error(err);

			res.send({status: 0, message: "Transcodes started", file: req.params.filename});
		  	CREATE_INDEX_M3U8();
		  	HD_720P_TRANSCODE(req.params.filename, uploadToGCS);
		  	SD_480P_TRANSCODE(req.params.filename, uploadToGCS);
		  	SD_360P_TRANSCODE(req.params.filename, uploadToGCS);
		  	SD_240P_TRANSCODE(req.params.filename, uploadToGCS);
		});


	});

	// Will not execute until all 4
	function uploadToGCS() {
		if(_transcodedRenditionsCount != 4) return;

		dir.files(_OUTPUT_PATH, function(err, files) {
			if (err) throw err;

			_total_files_count = files.length;
			_uploaded_files_count = 0; // Use this rather than index because indexes are called asynchronously

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
						_uploaded_files_count++;


						_ret = {'event': 'gcsupload', 'file': gFileObj.name, 'uploadedCount': _uploaded_files_count, 'totalCount': _total_files_count};
						wss.broadcast(JSON.stringify(_ret));
						console.log(JSON.stringify(_ret));
					});
				}
			});
		});
	}
}



server.listen(_PORT, function() {
	console.log('%s listening at %s', server.name, server.url);
});





