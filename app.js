var fs = require('fs-extra');
var restify = require('restify');
var ffmpeg = require('fluent-ffmpeg');
const WebSocket = require('ws');
var dir = require('node-dir');
var path = require('path');
var request = require('request');


var google_cloud = require('google-cloud')({
	projectId: 'broadcast-cx',
	keyFilename: 'keys/broadcast-cx-bda0296621a4.json'
});

var gcs = google_cloud.storage();
var bucket = gcs.bucket('broadcast-cx-raw-recordings');
var dest_bucket = gcs.bucket('broadcast-cx-sandbox');

var server = restify.createServer({
	name: 'ffmpeg-runner'
});

server.pre(restify.pre.sanitizePath());
server.use(restify.acceptParser(server.acceptable));
server.use(restify.queryParser({ mapParams: false })); // Parses URL queries, i.e. ?name=hello&gender=male
server.use(restify.bodyParser());
server.use(restify.gzipResponse()); // Gzip by default if accept-encoding: gzip is set on request
server.pre(restify.CORS()); // Enable CORS headers
server.use(restify.fullResponse());


// Semi-Hack: If environment path is same as GCS, then assume from GCS
if(process.env.PWD == '/usr/local/ffmpeg-runner/broadcast.cx-ffmpeg-runner') {
	_OUTPUT_PATH = '/usr/local/ffmpeg-runner/broadcast.cx-ffmpeg-runner/_outputs';
	_PRESETS_PATH = '/usr/local/ffmpeg-runner/broadcast.cx-ffmpeg-runner/presets';
	_PORT = 8080; // 8080 forwarded to 80 with iptables rule
	_WS_PORT = 8081;

} else { // Running local on development
	_OUTPUT_PATH = '/Users/XYK/Desktop/ffmpeg_outputs';
	_PRESETS_PATH = '/Users/XYK/Desktop/Dropbox/broadcast.cx-ffmpeg-runner/presets';
	ffmpeg.setFfmpegPath('/Users/XYK/Desktop/ffmpeg'); // Explicitly set ffmpeg and ffprobe paths
	ffmpeg.setFfprobePath('/Users/XYK/Desktop/ffprobe');
	_PORT = 8080;
	_WS_PORT = 8081;
}

const wss = new WebSocket.Server({ port: _WS_PORT });

wss.broadcast = function broadcast(data) {
	wss.clients.forEach(function each(client) {
		if(client.readyState === WebSocket.OPEN) {
			client.send(data);
		}
	});
};

wss.on('connection', function connection(ws) {
	ws.send(JSON.stringify({'event': 'success', 'message': 'Connected to ffmpeg-runner.'}));
});



_transcodeInProgress = false;

server.post('/transcode/hls/:filename', hlsTranscode);


function hlsTranscode(req, res, next) {
	if(!req.params.filename) {
		wss.broadcast(JSON.stringify({'event': 'error', 'message': 'Invalid request.'}));
		res.send(400, {status: 1, message: "Invalid Request."});
		return;
	}
	if(_transcodeInProgress) {
		wss.broadcast(JSON.stringify({'event': 'error', 'message': 'Already transcoding a file.'}));
		res.send(400, {status: 1, message: "Already transcoding a file."});
		return;
	}

	_transcodeInProgress = true;
	_transcodedRenditionsCount = 0;
	_trimmingOptions = [];


	if(req.body.startTime && req.body.endTime && req.body.endTime > req.body.startTime) {
		// If there is a startTime parameter and an endTime parameter, modify _trimmingOptions -ss (start) and -t (duration)
		_trimmingOptions = ['-ss ' + req.body.startTime, '-t ' + (req.body.endTime - req.body.startTime)];
		console.log(_trimmingOptions);
	}
	if(req.body.startTime && !req.body.endTime) {
		// If there is only a start time parameter, cut video from startTime
		_trimmingOptions = ['-ss ' + req.body.startTime];
		console.log(_trimmingOptions);
	}
	if((req.body.startTime > req.body.endTime) && req.body.endTime) {
		res.send(400, {status: 1, message: "Start time is after end time."});
		_ret = {'event': 'error', 'message': 'Start time is after end time.'};
		wss.broadcast(JSON.stringify(_ret));
		return;
	}

	HD_720P_TRANSCODE = function(filename, callback) { 
		_HD_720P = ffmpeg(filename, { presets: _PRESETS_PATH }).preset('hls')
		.videoBitrate(3000)
		.audioBitrate('96k')
		.size('1280x720')
		.inputOptions(_trimmingOptions)
		.on('start', function(commandLine) {
			_ret = {'event': 'm3u8', 'status': 'start', 'rendition': '720P_3000K', 'command': commandLine};

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
		    //console.log('Stderr output: ' + stderrLine);
		})
		.on('error', function(err, stdout, stderr) {
			_ret = {'event': 'error', 'message': err.message};
			wss.broadcast(JSON.stringify(_ret));
		})
		.on('end', function(stdout, stderr) {
			_ret = {'event': 'm3u8', 'status': 'complete', 'rendition': '720P_3000K'};
		    console.log(JSON.stringify(_ret));
		    wss.broadcast(JSON.stringify(_ret));

    	    ffmpeg(_OUTPUT_PATH + '/720p_3000k.m3u8') // Concatenate M3U8 playlist into MP4 with moovatom at front
    	    .outputOptions('-c', 'copy')
    	    .outputOptions('-bsf:a', 'aac_adtstoasc')
    	   	.outputOptions('-movflags', '+faststart')
    	   	.on('start', function(commandLine) {
    	   		_ret = {'event': 'mp4', 'status': 'start', 'rendition': '720P_3000K', 'command': commandLine};
    	   		wss.broadcast(JSON.stringify(_ret));
    	   	    console.log(JSON.stringify(_ret));
    	   	})
    	   	.on('end', function(stdout, stderr) {
       			_ret = {'event': 'mp4', 'status': 'complete', 'rendition': '720P_3000K'};
       			wss.broadcast(JSON.stringify(_ret));
       		    console.log(JSON.stringify(_ret));
       		    _transcodedRenditionsCount++;
       		    callback();
    	   	})
    	   	.saveToFile(_OUTPUT_PATH + '/720p_3000k.mp4')

		})
		.saveToFile(_OUTPUT_PATH + '/720p_3000k.m3u8');
	}

	SD_480P_TRANSCODE = function(filename, callback) {
		_SD_480P = ffmpeg(filename, { presets: _PRESETS_PATH }).preset('hls')
		.videoBitrate(1500)
		.audioBitrate('96k')
		.size('854x480')
		.inputOptions(_trimmingOptions)
		.on('start', function(commandLine) {
			_ret = {'event': 'm3u8', 'status': 'start', 'rendition': '480P_1500K', 'command': commandLine};

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
			_ret = {'event': 'm3u8', 'status': 'complete', 'rendition': '480P_1500K'};
		    console.log(JSON.stringify(_ret));
		    wss.broadcast(JSON.stringify(_ret));

		    ffmpeg(_OUTPUT_PATH + '/480p_1500k.m3u8') // Concatenate M3U8 playlist into MP4 with moovatom at front
		    .outputOptions('-c', 'copy')
		    .outputOptions('-bsf:a', 'aac_adtstoasc')
		   	.outputOptions('-movflags', '+faststart')
		   	.on('start', function(commandLine) {
		   		_ret = {'event': 'mp4', 'status': 'start', 'rendition': '480P_1500K', 'command': commandLine};
		   		wss.broadcast(JSON.stringify(_ret));
		   	    console.log(JSON.stringify(_ret));
		   	})
		   	.on('end', function(stdout, stderr) {
	   			_ret = {'event': 'mp4', 'status': 'complete', 'rendition': '480P_1500K'};
	   			wss.broadcast(JSON.stringify(_ret));
	   		    console.log(JSON.stringify(_ret));
	   		    _transcodedRenditionsCount++;
	   		    callback();
		   	})
		   	.saveToFile(_OUTPUT_PATH + '/480p_1500k.mp4')

		})
		.saveToFile(_OUTPUT_PATH + '/480p_1500k.m3u8');
	}

	SD_360P_TRANSCODE = function(filename, callback) {
		_SD_360P = ffmpeg(filename, { presets: _PRESETS_PATH }).preset('hls')
		.videoBitrate(850)
		.audioBitrate('96k')
		.size('640x360')
		.inputOptions(_trimmingOptions)
		.on('start', function(commandLine) {
			_ret = {'event': 'm3u8', 'status': 'start', 'rendition': '360P_850K', 'command': commandLine};

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
			_ret = {'event': 'm3u8', 'status': 'complete', 'rendition': '360P_850K'};
		    console.log(JSON.stringify(_ret));
		    wss.broadcast(JSON.stringify(_ret));

    	    ffmpeg(_OUTPUT_PATH + '/360p_850k.m3u8') // Concatenate M3U8 playlist into MP4 with moovatom at front
    	    .outputOptions('-c', 'copy')
    	    .outputOptions('-bsf:a', 'aac_adtstoasc')
    	   	.outputOptions('-movflags', '+faststart')
    	   	.on('start', function(commandLine) {
    	   		_ret = {'event': 'mp4', 'status': 'start', 'rendition': '360P_850K', 'command': commandLine};
    	   		wss.broadcast(JSON.stringify(_ret));
    	   	    console.log(JSON.stringify(_ret));
    	   	})
    	   	.on('end', function(stdout, stderr) {
       			_ret = {'event': 'mp4', 'status': 'complete', 'rendition': '360P_850K'};
       			wss.broadcast(JSON.stringify(_ret));
       		    console.log(JSON.stringify(_ret));
       		    _transcodedRenditionsCount++;
       		    callback();
    	   	})
    	   	.saveToFile(_OUTPUT_PATH + '/360p_850k.mp4')

		})
		.saveToFile(_OUTPUT_PATH + '/360p_850k.m3u8');
	}

	SD_240P_TRANSCODE = function(filename, callback) {
		_SD_240P = ffmpeg(filename, { presets: _PRESETS_PATH }).preset('hls')
		.videoBitrate(400)
		.audioBitrate('96k')
		.size('352x240')
		.inputOptions(_trimmingOptions)
		.on('start', function(commandLine) {
			_ret = {'event': 'm3u8', 'status': 'start', 'rendition': '240P_400K', 'command': commandLine};

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
			_ret = {'event': 'm3u8', 'status': 'complete', 'rendition': '240P_400K'};
		    console.log(JSON.stringify(_ret));
		    wss.broadcast(JSON.stringify(_ret));

		    ffmpeg(_OUTPUT_PATH + '/240p_400k.m3u8') // Concatenate M3U8 playlist into MP4 with moovatom at front
		    .outputOptions('-c', 'copy')
		    .outputOptions('-bsf:a', 'aac_adtstoasc')
		   	.outputOptions('-movflags', '+faststart')
		   	.on('start', function(commandLine) {
		   		_ret = {'event': 'mp4', 'status': 'start', 'rendition': '240P_400K', 'command': commandLine};
		   		wss.broadcast(JSON.stringify(_ret));
		   	    console.log(JSON.stringify(_ret));
		   	})
		   	.on('error', function(err, stdout, stderr) {
		   		console.log(err.message);
		   	})
		   	.on('end', function(stdout, stderr) {
	   			_ret = {'event': 'mp4', 'status': 'complete', 'rendition': '240P_400K'};
	   			wss.broadcast(JSON.stringify(_ret));
	   		    console.log(JSON.stringify(_ret));
	   		    _transcodedRenditionsCount++;
	   		    callback();
		   	})
		   	.saveToFile(_OUTPUT_PATH + '/240p_400k.mp4')

		})
		.saveToFile(_OUTPUT_PATH + '/240p_400k.m3u8');
	}

	CREATE_INDEX_M3U8 = function() {
		fs.createReadStream('index.m3u8').pipe(fs.createWriteStream(_OUTPUT_PATH + '/index.m3u8'));
	}

	CREATE_THUMBNAILS = function(filename, callback) {
		ffmpeg(filename)
		  	.on('filenames', function(filenames) {
				_ret = {'event': 'thumbnail', 'status': 'start', 'files': filenames};
				wss.broadcast(JSON.stringify(_ret));
				console.log(JSON.stringify(_ret));
		  	})
			.on('end', function() {
				_transcodedRenditionsCount++;
				_ret = {'event': 'thumbnail', 'status': 'complete'};
				wss.broadcast(JSON.stringify(_ret));
				console.log(JSON.stringify(_ret));
				callback();
			})
			.screenshots({
				// Will take screens at 20%, 40%, 60% and 80% of the video
				count: 5,
				folder: _OUTPUT_PATH,
				filename: 'thumbnail_%s_%00i_%r.jpg',
				size: '640x360'
			});
	}



	res.send({status: 0, message: "Starting transcode", file: req.params.filename});
	wss.broadcast(JSON.stringify({'event': 'gcsupload', 'uploadedCount': 0, 'totalCount': 0}));
	wss.broadcast(JSON.stringify({'event': 'mp4', 'status': 'pending', 'rendition': '240P_400K'}));
	wss.broadcast(JSON.stringify({'event': 'mp4', 'status': 'pending', 'rendition': '360P_850K'}));
	wss.broadcast(JSON.stringify({'event': 'mp4', 'status': 'pending', 'rendition': '480P_1500K'}));
	wss.broadcast(JSON.stringify({'event': 'mp4', 'status': 'pending', 'rendition': '720P_3000K'}));

	_ret = {'event': 'download', 'status': 'start', 'file': req.params.filename};
	console.log(JSON.stringify(_ret))
	wss.broadcast(JSON.stringify(_ret));

	bucket.file(req.params.filename).download({
		destination: req.params.filename
	}, function(err) {
		if(err) { // Error handling (bucket file not found in GCS)
			console.log(err.code, err.message);
			wss.broadcast(JSON.stringify({'event': 'download', 'status': 'error', 'message': err.code + ' ' + err.message}));
			res.send({'event': 'download', 'status': 'error', 'message': err.code + ' ' + err.message});
			return;
		}

		_ret = {'event': 'download', 'status': 'complete', 'file': req.params.filename};
		console.log(JSON.stringify(_ret))
		wss.broadcast(JSON.stringify(_ret));

		fs.emptyDir(_OUTPUT_PATH, err => { // Clear out output path of old m3u8 files
			if (err) return console.error(err);

		  	CREATE_INDEX_M3U8();
		  	HD_720P_TRANSCODE(req.params.filename, uploadToGCS);
		  	SD_480P_TRANSCODE(req.params.filename, uploadToGCS);
		  	SD_360P_TRANSCODE(req.params.filename, uploadToGCS);
		  	SD_240P_TRANSCODE(req.params.filename, uploadToGCS);
		  	CREATE_THUMBNAILS(req.params.filename, uploadToGCS);
		});


	});

	// Will not execute until all 5
	function uploadToGCS() {
		if(_transcodedRenditionsCount != 5) return;
		_transcodeInProgress = false; // End transcode in progress flag

		dir.files(_OUTPUT_PATH, function(err, files) {
			if (err) throw err;

			_total_files_count = files.length;
			_uploaded_files_count = 0; // Use this rather than index because indexes are called asynchronously

			files.forEach(function(file, index) {
				if(path.extname(file) === '.m3u8' || path.extname(file) === '.ts' || path.extname(file) === '.jpg' || path.extname(file) === '.mp4') {
					// Only upload M3U8s and transport streams

					var _options = { // GCS destination bucket folder and file paths
					  destination: path.basename(req.params.filename, '.mp4') + '/' + path.basename(file) // Directory of /filenamewithoutextension/file
					};

					dest_bucket.upload(file, _options, function(err, gFileObj) {
						if(err) { return console.log(err); }

						if(gFileObj.name.indexOf('.m3u8') != -1) {
							var metadata = { contentType: 'application/x-mpegURL' };
						} else if(gFileObj.name.indexOf('.ts') != -1) {
							var metadata = { contentType: 'video/MP2T' };
						} else if(gFileObj.name.indexOf('.jpg') != -1) {
							var metadata = { contentType: 'image/jpeg' };
						} else {
							var metadata = { contentType: 'video/mp4' };
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





