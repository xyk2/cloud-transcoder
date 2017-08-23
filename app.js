var fs = require('fs-extra');
var restify = require('restify');
var ffmpeg = require('fluent-ffmpeg');
const WebSocket = require('ws');
var dir = require('node-dir');
var path = require('path');
var request = require('request');
var async = require('async');
const uuidv4 = require('uuid/v4')

var MIME_TYPE_LUT = {
	'.m3u8': 'application/x-mpegURL',
	'.ts': 'video/MP2T',
	'.jpg': 'image/jpeg',
	'.mp4': 'video/mp4',
	'.m4a': 'audio/mp4'
}
var _transcodeInProgress = false;

var google_cloud = require('google-cloud')({
	projectId: 'broadcast-cx',
	keyFilename: 'keys/broadcast-cx-bda0296621a4.json'
});


var gcs = google_cloud.storage();
var bucket = gcs.bucket('broadcast-cx-raw-recordings');
var dest_bucket = gcs.bucket('cx-video-content');
//var dest_bucket = gcs.bucket('cx-videos');

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
	_INPUT_PATH = '/usr/local/ffmpeg-runner/broadcast.cx-ffmpeg-runner/_inputs';
	_OUTPUT_PATH = '/usr/local/ffmpeg-runner/broadcast.cx-ffmpeg-runner/_outputs';
	_PRESETS_PATH = '/usr/local/ffmpeg-runner/broadcast.cx-ffmpeg-runner/presets';
	_API_HOST = 'http://api.broadcast.cx';
	_PORT = 8080; // 8080 forwarded to 80 with iptables rule
	_WS_PORT = 8081;

} else { // Running local on development
	_INPUT_PATH = '/Users/XYK/Desktop/Dropbox/broadcast.cx-ffmpeg-runner/_inputs';
	_OUTPUT_PATH = '/Users/XYK/Desktop/Dropbox/broadcast.cx-ffmpeg-runner/_outputs';
	_PRESETS_PATH = '/Users/XYK/Desktop/Dropbox/broadcast.cx-ffmpeg-runner/presets';
	_API_HOST = 'http://local.broadcast.cx:8088';
	ffmpeg.setFfmpegPath('/Users/XYK/Desktop/ffmpeg'); // Explicitly set ffmpeg and ffprobe paths
	ffmpeg.setFfprobePath('/Users/XYK/Desktop/ffprobe');
	_PORT = 8080;
	_WS_PORT = 8081;
}

const wss = new WebSocket.Server({ port: _WS_PORT });

wss.broadcast = function broadcast(data) {
	console.log(data);
	wss.clients.forEach(function each(client) {
		if(client.readyState === WebSocket.OPEN) {
			client.send(data);
		}
	});
};

wss.on('connection', function connection(ws) {
	ws.send(JSON.stringify({'event': 'success', 'message': 'Connected to ffmpeg-runner.'}));
});


server.use(function(req, res, next) { // If transcode already in progress, throw error
	if(_transcodeInProgress) {
		wss.broadcast(JSON.stringify({'event': 'error', 'message': 'Already transcoding a file.'}));
		res.send(400, {status: 1, message: "Already transcoding a file."});
		return;
	}
	next();
})

server.post('/transcode/hls/:filename', hlsTranscode);
server.post('/transcode/highlights/:filename', highlights);
server.post('/transcode/highlightReel/:filename', highlightReel);
server.post('/transcode/fullgame/:filename', fullGameTranscodeToMP4HLS);


function hlsTranscode(req, res, next) {
	if(!req.params.filename) {
		wss.broadcast(JSON.stringify({'event': 'error', 'message': 'Invalid request.'}));
		res.send(400, {status: 1, message: "Invalid Request."});
		return;
	}

	_transcodeInProgress = true;
	_transcodedRenditionsCount = 0;
	_trimmingOptions = [];
	_thumbnailFiles = [];
	_MP4Files = [];


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
		wss.broadcast(JSON.stringify({'event': 'error', 'message': 'Start time is after end time.'}));
		return;
	}

	HD_720P_TRANSCODE = function(filename, callback) { 
		_HD_720P = ffmpeg(filename, { presets: _PRESETS_PATH }).preset('hls')
		.videoBitrate(3000)
		//.audioBitrate('96k')
		.size('1280x720')
		.inputOptions(_trimmingOptions)
		.on('start', function(commandLine) {
		    wss.broadcast(JSON.stringify({'event': 'm3u8', 'status': 'start', 'rendition': '720P_3000K', 'command': commandLine}));

		})
		.on('progress', function(progress) {
			progress['event'] = 'progress';
			progress['rendition'] = '720P_3000K';			
			wss.broadcast(JSON.stringify(progress));
		})
		.on('stderr', function(stderrLine) {
		    //console.log('Stderr output: ' + stderrLine);
		})
		.on('error', function(err, stdout, stderr) {
			wss.broadcast(JSON.stringify({'event': 'error', 'message': err.message}));
		})
		.on('end', function(stdout, stderr) {
		    wss.broadcast(JSON.stringify({'event': 'm3u8', 'status': 'complete', 'rendition': '720P_3000K'}));

    	    ffmpeg(_OUTPUT_PATH + '/720p_3000k.m3u8') // Concatenate M3U8 playlist into MP4 with moovatom at front
    	    .outputOptions('-c', 'copy')
    	    .outputOptions('-bsf:a', 'aac_adtstoasc')
    	   	.outputOptions('-movflags', '+faststart')
    	   	.on('start', function(commandLine) {
    	   		wss.broadcast(JSON.stringify({'event': 'mp4', 'status': 'start', 'rendition': '720P_3000K', 'command': commandLine}));
    	   	})
    	   	.on('end', function(stdout, stderr) {
       			wss.broadcast(JSON.stringify({'event': 'mp4', 'status': 'complete', 'rendition': '720P_3000K'}));

       		    _MP4Files.push({
       		    	targetBitrate: 3000,
       		    	targetDisplayWidth: 1280,
       		    	targetDisplayHeight: 720,
       		    	description: "720p HD",
       		    	videoUrl: '720p_3000k.mp4'
       		    });

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
		//.audioBitrate('96k')
		.size('854x480')
		.inputOptions(_trimmingOptions)
		.on('start', function(commandLine) {
		    wss.broadcast(JSON.stringify({'event': 'm3u8', 'status': 'start', 'rendition': '480P_1500K', 'command': commandLine}));
		})
		.on('progress', function(progress) {			
			progress['event'] = 'progress';
			progress['rendition'] = '480P_1500K';			
			wss.broadcast(JSON.stringify(progress));
		})
		.on('stderr', function(stderrLine) {
		    //console.log('Stderr output: ' + stderrLine);
		})
		.on('error', function(err, stdout, stderr) {
			wss.broadcast(JSON.stringify({'event': 'error', 'message': err.message}));
		})
		.on('end', function(stdout, stderr) {
		    wss.broadcast(JSON.stringify({'event': 'm3u8', 'status': 'complete', 'rendition': '480P_1500K'}));

		    ffmpeg(_OUTPUT_PATH + '/480p_1500k.m3u8') // Concatenate M3U8 playlist into MP4 with moovatom at front
		    .outputOptions('-c', 'copy')
		    .outputOptions('-bsf:a', 'aac_adtstoasc')
		   	.outputOptions('-movflags', '+faststart')
		   	.on('start', function(commandLine) {
		   		wss.broadcast(JSON.stringify({'event': 'mp4', 'status': 'start', 'rendition': '480P_1500K', 'command': commandLine}));
		   	})
		   	.on('end', function(stdout, stderr) {
	   			wss.broadcast(JSON.stringify({'event': 'mp4', 'status': 'complete', 'rendition': '480P_1500K'}));
	   		    
	   		    _MP4Files.push({
	   		    	targetBitrate: 1500,
	   		    	targetDisplayWidth: 854,
	   		    	targetDisplayHeight: 480,
	   		    	description: "480p SD",
	   		    	videoUrl: '480p_1500k.mp4'
	   		    });

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
		//.audioBitrate('96k')
		.size('640x360')
		.inputOptions(_trimmingOptions)
		.on('start', function(commandLine) {
		    wss.broadcast(JSON.stringify({'event': 'm3u8', 'status': 'start', 'rendition': '360P_850K', 'command': commandLine}));
		})
		.on('progress', function(progress) {			
			progress['event'] = 'progress';
			progress['rendition'] = '360P_850K';			
			wss.broadcast(JSON.stringify(progress));
		})
		.on('stderr', function(stderrLine) {
		    //console.log('Stderr output: ' + stderrLine);
		})
		.on('error', function(err, stdout, stderr) {
			wss.broadcast(JSON.stringify({'event': 'error', 'message': err.message}));
		})
		.on('end', function(stdout, stderr) {
		    wss.broadcast(JSON.stringify({'event': 'm3u8', 'status': 'complete', 'rendition': '360P_850K'}));

    	    ffmpeg(_OUTPUT_PATH + '/360p_850k.m3u8') // Concatenate M3U8 playlist into MP4 with moovatom at front
    	    .outputOptions('-c', 'copy')
    	    .outputOptions('-bsf:a', 'aac_adtstoasc')
    	   	.outputOptions('-movflags', '+faststart')
    	   	.on('start', function(commandLine) {
    	   		wss.broadcast(JSON.stringify({'event': 'mp4', 'status': 'start', 'rendition': '360P_850K', 'command': commandLine}));
    	   	})
    	   	.on('end', function(stdout, stderr) {
       			wss.broadcast(JSON.stringify({'event': 'mp4', 'status': 'complete', 'rendition': '360P_850K'}));
       		    
       		    _MP4Files.push({
       		    	targetBitrate: 850,
       		    	targetDisplayWidth: 640,
       		    	targetDisplayHeight: 360,
       		    	description: "360p SD",
       		    	videoUrl: '360p_850k.mp4'
       		    });

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
		//.audioBitrate('96k')
		.size('352x240')
		.inputOptions(_trimmingOptions)
		.on('start', function(commandLine) {
		    wss.broadcast(JSON.stringify({'event': 'm3u8', 'status': 'start', 'rendition': '240P_400K', 'command': commandLine}));
		})
		.on('progress', function(progress) {			
			progress['event'] = 'progress';
			progress['rendition'] = '240P_400K';			
			wss.broadcast(JSON.stringify(progress));
		})
		.on('stderr', function(stderrLine) {
		    //console.log('Stderr output: ' + stderrLine);
		})
		.on('error', function(err, stdout, stderr) {
			wss.broadcast(JSON.stringify({'event': 'error', 'message': err.message}));
		})
		.on('end', function(stdout, stderr) {
		    wss.broadcast(JSON.stringify({'event': 'm3u8', 'status': 'complete', 'rendition': '240P_400K'}));

		    ffmpeg(_OUTPUT_PATH + '/240p_400k.m3u8') // Concatenate M3U8 playlist into MP4 with moovatom at front
		    .outputOptions('-c', 'copy')
		    .outputOptions('-bsf:a', 'aac_adtstoasc')
		   	.outputOptions('-movflags', '+faststart')
		   	.on('start', function(commandLine) {
		   		wss.broadcast(JSON.stringify({'event': 'mp4', 'status': 'start', 'rendition': '240P_400K', 'command': commandLine}));
		   	})
		   	.on('error', function(err, stdout, stderr) {
		   		console.log(err.message);
		   	})
		   	.on('end', function(stdout, stderr) {
	   			wss.broadcast(JSON.stringify({'event': 'mp4', 'status': 'complete', 'rendition': '240P_400K'}));

	   			_MP4Files.push({
	   				targetBitrate: 400,
	   				targetDisplayWidth: 352,
	   				targetDisplayHeight: 240,
	   				description: "240p SD",
	   				videoUrl: '240p_400k.mp4'
	   			});

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
				wss.broadcast(JSON.stringify({'event': 'thumbnail', 'status': 'start', 'files': filenames}));
				for(var index in filenames) {
					_thumbnailFiles.push({
						width: 640,
						height: 360,
						thumbnailUrl: filenames[index]
					});
				}
		  	})
			.on('end', function() {
				_transcodedRenditionsCount++;
				wss.broadcast(JSON.stringify({'event': 'thumbnail', 'status': 'complete'}));
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

	BEGIN_TRANSCODES = function() {
		fs.emptyDir(_OUTPUT_PATH, err => { // Clear out output path of old m3u8 files
			if (err) return console.error(err);

		  	CREATE_INDEX_M3U8();
		  	HD_720P_TRANSCODE(req.params.filename, uploadToGCS);
		  	SD_480P_TRANSCODE(req.params.filename, uploadToGCS);
		  	SD_360P_TRANSCODE(req.params.filename, uploadToGCS);
		  	SD_240P_TRANSCODE(req.params.filename, uploadToGCS);
		  	CREATE_THUMBNAILS(req.params.filename, uploadToGCS);
		});
	}

	res.send({status: 0, message: "Starting transcode", file: req.params.filename});
	wss.broadcast(JSON.stringify({'event': 'gcsupload', 'uploadedCount': 0, 'totalCount': 0}));
	wss.broadcast(JSON.stringify({'event': 'mp4', 'status': 'pending', 'rendition': '240P_400K'}));
	wss.broadcast(JSON.stringify({'event': 'mp4', 'status': 'pending', 'rendition': '360P_850K'}));
	wss.broadcast(JSON.stringify({'event': 'mp4', 'status': 'pending', 'rendition': '480P_1500K'}));
	wss.broadcast(JSON.stringify({'event': 'mp4', 'status': 'pending', 'rendition': '720P_3000K'}));
	wss.broadcast(JSON.stringify({'event': 'download', 'status': 'start', 'file': req.params.filename}));


	if(fs.existsSync(req.params.filename)) { // If file already downloaded in local directory, use that instead of downloading again
		wss.broadcast(JSON.stringify({'event': 'download', 'status': 'complete', 'file': req.params.filename}));
		BEGIN_TRANSCODES();
	} else {
		bucket.file(req.params.filename).download({
			destination: req.params.filename
		}, function(err) {
			if(err) { // Error handling (bucket file not found in GCS)
				wss.broadcast(JSON.stringify({'event': 'download', 'status': 'error', 'message': err.code + ' ' + err.message}));
				res.send({'event': 'download', 'status': 'error', 'message': err.code + ' ' + err.message});
				return;
			}

			BEGIN_TRANSCODES();
			wss.broadcast(JSON.stringify({'event': 'download', 'status': 'complete', 'file': req.params.filename}));
		});
	}


	// Will not execute until all 5
	function uploadToGCS() {
		if(_transcodedRenditionsCount != 5) return;
		_transcodeInProgress = false; // End transcode in progress flag

		function gcs_upload(file, options, uuid) {
			dest_bucket.upload(file, options, function(err, gFileObj) {
				if(err) { 
					//return console.log(err);
					console.log("File upload failed for " + file + ", trying again.");
					gcs_upload(file, options, uuid); // retry if error
					return;
				}

				if(gFileObj.name.indexOf('.m3u8') != -1) {
					var metadata = { contentType: 'application/x-mpegURL' };
				} else if(gFileObj.name.indexOf('.ts') != -1) {
					var metadata = { contentType: 'video/MP2T' };
				} else if(gFileObj.name.indexOf('.jpg') != -1) {
					var metadata = { contentType: 'image/jpeg' };
				} else {
					var metadata = { contentType: 'video/mp4' };
				}
				metadata.cacheControl = 'public, max-age=31556926';
				
				gFileObj.setMetadata(metadata, function(err, apiResponse) {});
				_uploadedFilesCount++;

				_ret = {'event': 'gcsupload', 'file': gFileObj.name, 'uploadedCount': _uploadedFilesCount, 'totalCount': _total_files_count};
				wss.broadcast(JSON.stringify(_ret));

				postToBroadcastCXLibrary(_uploadedFilesCount, _total_files_count, uuid);
			});
		}

		request.post({uri: _API_HOST + '/videos', json: req.body.api}, function(err, response, body) {
			if (err) return console.error(err);
			_GCS_BASEPATH = path.basename(req.params.filename, '.mp4') + '/' + body.uuid + '/';

			 dir.files(_OUTPUT_PATH, function(err, files) {
			 	if (err) throw err;

			 	_total_files_count = files.length;
			 	_uploadedFilesCount = 0; // Use this rather than index because indexes are called asynchronously

			 	files.forEach(function(file, index) {
			 		if(path.extname(file) != '.m3u8' && path.extname(file) != '.ts' && path.extname(file) != '.jpg' && path.extname(file) != '.mp4') return;
			 		// Only upload M3U8s and transport streams

			 		setTimeout(function() { // Sequence file uploads every 10ms to avoid socket timeouts
			 			var _options = { // GCS destination bucket folder and file paths
			 				resumable: false, // Disable resumable uploads (default is true for files >5MB). Socket hangup issues fix
			 				validation: false, // Disable crc32/md5 checksum validation 
			 				destination: _GCS_BASEPATH + path.basename(file) // Directory of /filenamewithoutextension/file
			 			};

			 			gcs_upload(file, _options, body.uuid);

			 		}, index * 10);
			 		
			 	});
			 });

		});
	}

	function postToBroadcastCXLibrary(uploadedCount, totalCount, uuid) {
		if(uploadedCount != totalCount) return;
		console.log("postToBroadcastCXLibrary");

		_PUT_BODY = req.body.api;
		_PUT_BODY['masterPlaylistUrl'] = 'http://cdn-google.broadcast.cx/' + _GCS_BASEPATH + 'index.m3u8';

		request.put({uri: _API_HOST + '/videos/' + uuid, json: _PUT_BODY}, function(err, response, body) {
			if (err) return console.error(err);

			console.log(body);

			for(var index in _thumbnailFiles) {
				_thumbnailFiles[index]['thumbnailUrl'] = 'http://cdn-google.broadcast.cx/' + _GCS_BASEPATH + _thumbnailFiles[index]['thumbnailUrl'];
				request.post({uri: _API_HOST + '/thumbnails/' + uuid, json: _thumbnailFiles[index]}, function(err, response, body) {
					if (err) return console.error(err);
					console.log(body);
				});
			}

			for(var index in _MP4Files) {
				_MP4Files[index]['videoUrl'] = 'http://cdn-google.broadcast.cx/' + _GCS_BASEPATH + _MP4Files[index]['videoUrl'];
				request.post({uri: _API_HOST + '/videoTranscodeProfiles/' + uuid, json: _MP4Files[index]}, function(err, response, body) {
					if (err) return console.error(err);
					console.log(body);
				});
			}

		});

	}
}

function highlights(req, res, next) {
	if(!req.params.filename || req.body.highlights.length == 0) {
		wss.broadcast(JSON.stringify({'event': 'error', 'message': 'Invalid request.'}));
		res.send(400, {status: 1, message: "Invalid Request."});
		return;
	}

	_transcodedRenditionsCount = 0;
	_uploadedFilesCount = 0;
	_totalTranscodedRenditionsCount = req.body.highlights.length;

	function gcs_upload(file, options, uuid) {
		dest_bucket.upload(file, options, function(err, gFileObj) {
			if(err) { 
				console.log("File upload failed for " + file + ", trying again.");
				gcs_upload(file, options, uuid); // retry if error
				return;
			}

			if(gFileObj.name.indexOf('.jpg') != -1) {
				var metadata = { contentType: 'image/jpeg' };
			} else {
				var metadata = { contentType: 'video/mp4' };
			}
			metadata.cacheControl = 'public, max-age=31556926';
			
			gFileObj.setMetadata(metadata, function(err, apiResponse) {});
			_uploadedFilesCount++;

			_ret = {'event': 'gcsupload', 'file': gFileObj.name, 'uploadedCount': _uploadedFilesCount, 'totalCount': _totalTranscodedRenditionsCount * 2};
			wss.broadcast(JSON.stringify(_ret));
		});
	}

	CUT_HIGHLIGHT = function(filename, trimmingOptions, gcsFilename, gcsThumbnailFilename, highlightParameters, callback) { 
		_HD_720P = ffmpeg(filename, { presets: _PRESETS_PATH }).preset('highlight_mp4')
		.videoBitrate(2000)
		.inputOptions(trimmingOptions)
		.on('start', function(commandLine) {
		    wss.broadcast(JSON.stringify({'event': 'highlight', 'status': 'start', 'command': commandLine}));
		})
		.on('progress', function(progress) {
			//progress['event'] = 'progress';
			//progress['rendition'] = '720P_3000K';			
			//wss.broadcast(JSON.stringify(progress));
			console.log(JSON.stringify(progress));
		})
		.on('stderr', function(stderrLine) {
		    //console.log('Stderr output: ' + stderrLine);
		})
		.on('error', function(err, stdout, stderr) {
			wss.broadcast(JSON.stringify({'event': 'error', 'message': err.message}));
		})
		.on('end', function(stdout, stderr) {
			ffmpeg(_OUTPUT_PATH + '/' + gcsFilename)
			.on('filenames', function(filenames) {
		  	})
			.on('end', function() {
				_transcodedRenditionsCount++;
				
				_PUT_BODY = {
					videoUrl: highlightParameters.videoUrl,
					thumbnailUrl: highlightParameters.thumbnailUrl
				};

				request.put({uri: _API_HOST + '/manualLiveMarkedHighlights/' + highlightParameters.uuid, json: _PUT_BODY}, function(err, response, body) {
					if (err) return console.error(err);
					console.log(body);
				});

				gcs_upload(_OUTPUT_PATH + '/' + gcsFilename, {resumable: false, validation: false, destination: 'highlights/' + path.basename(gcsFilename)}, '');
				gcs_upload(_OUTPUT_PATH + '/' + gcsThumbnailFilename, {resumable: false, validation: false, destination: 'highlights/' + path.basename(gcsThumbnailFilename)}, '');

				wss.broadcast(JSON.stringify({'event': 'highlight', 'status': 'complete', 'completedCount': _transcodedRenditionsCount, 'totalCount': _totalTranscodedRenditionsCount}));
				callback();
			})
			.screenshots({
				// Will take screens at 20%, 40%, 60% and 80% of the video
				count: 1,
				folder: _OUTPUT_PATH,
				filename: gcsThumbnailFilename,
				size: '512x288'
			});	
		})
		.saveToFile(_OUTPUT_PATH + '/' + gcsFilename);
	}

	BEGIN_TRANSCODES = function() {
		fs.emptyDir(_OUTPUT_PATH, err => { // Clear out output path of old files
			if (err) return console.error(err);

			for(var x in req.body.highlights) {
				// Output filename format: startTime_lastBlockOfUUID_description.mp4 (spaces are replaced by underscores)
				_gcsFilename = (req.body.highlights[x].startTime/1000).toFixed(0) + '_' + req.body.highlights[x].uuid.split('-')[4] + '_' + (req.body.highlights[x].description || "") + '.mp4';
				_gcsFilename = _gcsFilename.replace(' ', '_');
				_gcsThumbnailFilename = _gcsFilename.replace('.mp4', '.jpg');

				_trimLength = (req.body.highlights[x].endTime - req.body.highlights[x].startTime)/1000;
				_trimmingOptions = ['-ss ' + (req.body.highlights[x].startTime/1000).toFixed(1), '-t ' + _trimLength];
				
				req.body.highlights[x].videoUrl = 'http://cdn-google.broadcast.cx/highlights/' + _gcsFilename;
				req.body.highlights[x].thumbnailUrl = 'http://cdn-google.broadcast.cx/highlights/' + _gcsThumbnailFilename;
				
				if(_trimLength > 1) {
				// Ensure no 0 lengths
					CUT_HIGHLIGHT(req.params.filename, _trimmingOptions, _gcsFilename, _gcsThumbnailFilename, req.body.highlights[x]);
				}
			}
		});
	}



	res.send({status: 0, message: "Starting highlights transcode", file: req.params.filename, count: _totalTranscodedRenditionsCount});
	wss.broadcast(JSON.stringify({'event': 'gcsupload', 'uploadedCount': 0, 'totalCount': 0}));
	wss.broadcast(JSON.stringify({'event': 'download', 'status': 'start', 'file': req.params.filename}));
	wss.broadcast(JSON.stringify({'event': 'highlight', 'status': 'complete', 'completedCount': 0, 'totalCount': _totalTranscodedRenditionsCount}));


	if(fs.existsSync(req.params.filename)) { // If file already downloaded in local directory, use that instead of downloading again
		wss.broadcast(JSON.stringify({'event': 'download', 'status': 'complete', 'file': req.params.filename}));
		BEGIN_TRANSCODES();

	} else { // If doesn't exist then download from GCS
		bucket.file(req.params.filename).download({
			destination: req.params.filename
		}, function(err) {
			if(err) { // Error handling (bucket file not found in GCS)
				wss.broadcast(JSON.stringify({'event': 'download', 'status': 'error', 'message': err.code + ' ' + err.message}));
				res.send({'event': 'download', 'status': 'error', 'message': err.code + ' ' + err.message});
				return;
			}

			wss.broadcast(JSON.stringify({'event': 'download', 'status': 'complete', 'file': req.params.filename}));
			BEGIN_TRANSCODES();
		});
	}
}

function highlightReel(req, res, next) {
	if(!req.params.filename || req.body.highlights.length == 0) {
		wss.broadcast(JSON.stringify({'event': 'error', 'message': 'Invalid request.'}));
		res.send(400, {status: 1, message: "Invalid Request."});
		return;
	}

	_transcodedRenditionsCount = 0;
	_uploadedFilesCount = 0;
	_concatenate_playlist = ''; // txt file for ffmpeg concatenate command
	_totalTranscodedRenditionsCount = req.body.highlights.length;

	function gcs_upload(file, options, uuid) {
		dest_bucket.upload(file, options, function(err, gFileObj) {
			if(err) { 
				console.log("File upload failed for " + file + ", trying again.");
				gcs_upload(file, options, uuid); // retry if error
				return;
			}

			if(gFileObj.name.indexOf('.jpg') != -1) {
				var metadata = { contentType: 'image/jpeg' };
			} else {
				var metadata = { contentType: 'video/mp4' };
			}
			metadata.cacheControl = 'public, max-age=31556926';
			
			gFileObj.setMetadata(metadata, function(err, apiResponse) {});
			_uploadedFilesCount++;

			_ret = {'event': 'gcsupload', 'file': gFileObj.name, 'uploadedCount': _uploadedFilesCount, 'totalCount': _totalTranscodedRenditionsCount * 2};
			wss.broadcast(JSON.stringify(_ret));
		});
	}

	CUT_HIGHLIGHT = function(filename, trimmingOptions, gcsFilename, callback) {
		_HD_720P = ffmpeg(filename, { presets: _PRESETS_PATH }).preset('highlight_mp4')
		.videoBitrate(3000)
		.inputOptions(trimmingOptions)
		.on('start', function(commandLine) {
		    wss.broadcast(JSON.stringify({'event': 'highlight', 'status': 'start', 'command': commandLine}));
		})
		.on('progress', function(progress) {
			console.log(JSON.stringify(progress));
		})
		.on('stderr', function(stderrLine) {
		    //console.log('Stderr output: ' + stderrLine);
		})
		.on('error', function(err, stdout, stderr) {
			wss.broadcast(JSON.stringify({'event': 'error', 'message': err.message}));
		})
		.on('end', function(stdout, stderr) {
			_transcodedRenditionsCount++;
			wss.broadcast(JSON.stringify({'event': 'highlight', 'status': 'complete', 'completedCount': _transcodedRenditionsCount, 'totalCount': _totalTranscodedRenditionsCount}));
			callback();
		})
		.saveToFile(_OUTPUT_PATH + '/' + gcsFilename);
	}

	CONCATENATE_HIGHLIGHTS = function() {
		if(_transcodedRenditionsCount != _totalTranscodedRenditionsCount) return;

		_output_filename = 'highlightReel_' + Math.floor(new Date() / 1000) + '_' + req.body.api.gameId + '.mp4';
		fs.writeFileSync('_concatenate_playlist.txt', _concatenate_playlist, 'utf-8');

		ffmpeg('_concatenate_playlist.txt')
			.inputOptions('-f', 'concat')
			.inputOptions('-safe', '0')
			.outputOptions('-c', 'copy')
			.on('start', function(commandLine) {
			    wss.broadcast(JSON.stringify({'event': 'highlightReel', 'status': 'start', 'command': commandLine}));
			})
			.on('progress', function(progress) {
				console.log(JSON.stringify(progress));
			})
			.on('stderr', function(stderrLine) {
			    //onsole.log('Stderr output: ' + stderrLine);
			})
			.on('error', function(err, stdout, stderr) {
				wss.broadcast(JSON.stringify({'event': 'error', 'message': err.message}));
			})
			.on('end', function(stdout, stderr) {
				wss.broadcast(JSON.stringify({'event': 'highlightReel', 'status': 'complete', 'filename': _output_filename}));

				request.post({uri: 'http://127.0.0.1:' + _PORT + '/transcode/hls/' + _output_filename, json: req.body}, function(err, response, body) {
					if (err) return console.error(err);
					//console.log(response);
				});


			})
			.saveToFile(_output_filename);

	}

	BEGIN_TRANSCODES = function() {
		fs.emptyDir(_OUTPUT_PATH, err => { // Clear out output path of old files
			if (err) return console.error(err);

			for(var x in req.body.highlights) {
				// Output filename format: startTime_lastBlockOfUUID_description.mp4 (spaces are replaced by underscores)
				_gcsFilename = (req.body.highlights[x].startTime/1000).toFixed(0) + '_' + req.body.highlights[x].uuid.split('-')[4] + '_' + (req.body.highlights[x].description || "") + '.mp4';
				_gcsFilename = _gcsFilename.replace(' ', '_');

				_trimLength = (req.body.highlights[x].endTime - req.body.highlights[x].startTime)/1000;
				_trimmingOptions = ['-ss ' + (req.body.highlights[x].startTime/1000).toFixed(1), '-t ' + _trimLength];
				_concatenate_playlist += "file '" + _OUTPUT_PATH + '/' + _gcsFilename + "'\n";

				if(_trimLength > 1) { // Ensure no 0 lengths
					CUT_HIGHLIGHT(req.params.filename, _trimmingOptions, _gcsFilename, CONCATENATE_HIGHLIGHTS);
				}
			}
		});
		
	}



	res.send({status: 0, message: "Starting highlights transcode", file: req.params.filename, count: _totalTranscodedRenditionsCount});
	wss.broadcast(JSON.stringify({'event': 'gcsupload', 'uploadedCount': 0, 'totalCount': 0}));
	wss.broadcast(JSON.stringify({'event': 'download', 'status': 'start', 'file': req.params.filename}));
	wss.broadcast(JSON.stringify({'event': 'highlight', 'status': 'complete', 'completedCount': 0, 'totalCount': _totalTranscodedRenditionsCount}));


	if(fs.existsSync(req.params.filename)) { // If file already downloaded in local directory, use that instead of downloading again
		wss.broadcast(JSON.stringify({'event': 'download', 'status': 'complete', 'file': req.params.filename}));
		BEGIN_TRANSCODES();

	} else { // If doesn't exist then download from GCS
		bucket.file(req.params.filename).download({
			destination: req.params.filename
		}, function(err) {
			if(err) { // Error handling (bucket file not found in GCS)
				wss.broadcast(JSON.stringify({'event': 'download', 'status': 'error', 'message': err.code + ' ' + err.message}));
				res.send({'event': 'download', 'status': 'error', 'message': err.code + ' ' + err.message});
				return;
			}

			wss.broadcast(JSON.stringify({'event': 'download', 'status': 'complete', 'file': req.params.filename}));
			BEGIN_TRANSCODES();
		});
	}
}

function fullGameTranscodeToMP4HLS(req, res, next) {
	if(!req.params.filename) {
		wss.broadcast(JSON.stringify({'event': 'error', 'message': 'Invalid request.'}));
		res.send(400, {status: 1, message: "Invalid Request."});
		return;
	}

	MASTER_GAME_FOOTAGE_HLS(req.params.filename);

	res.send({status: 0, message: "Starting transcode", file: req.params.filename});
}




HD_720P_TRANSCODE = function(filename, prefix, callback) { 
	_HD_720P = ffmpeg(filename, { presets: _PRESETS_PATH }).preset('hls')
	.videoBitrate(3000)
	.audioBitrate('128k')
	.size('1280x720')
	.on('start', function(commandLine) {
	    wss.broadcast(JSON.stringify({'event': 'm3u8', 'status': 'start', 'rendition': '720P_3000K', 'command': commandLine}));

	})
	.on('progress', function(progress) {
		progress['event'] = 'progress';
		progress['rendition'] = '720P_3000K';			
		wss.broadcast(JSON.stringify(progress));
	})
	.on('stderr', function(stderrLine) {
	    //console.log('Stderr output: ' + stderrLine);
	})
	.on('error', function(err, stdout, stderr) {
		wss.broadcast(JSON.stringify({'event': 'error', 'message': err.message}));
	})
	.on('end', function(stdout, stderr) {
	    wss.broadcast(JSON.stringify({'event': 'm3u8', 'status': 'complete', 'rendition': '720P_3000K'}));

	    ffmpeg(_OUTPUT_PATH + '/hls/720p_3000k.m3u8', { presets: _PRESETS_PATH }) // Concatenate M3U8 playlist into MP4 with moovatom at front
	    .preset('m3u8_to_mp4')
	   	.on('start', function(commandLine) {
	   		wss.broadcast(JSON.stringify({'event': 'mp4', 'status': 'start', 'rendition': '720P_3000K', 'command': commandLine}));
	   	})
	   	.on('end', function(stdout, stderr) {
   			wss.broadcast(JSON.stringify({'event': 'mp4', 'status': 'complete', 'rendition': '720P_3000K'}));
   		
   		    return callback(null, prefix + '_720p_3000k.mp4');
	   	})
	   	.saveToFile(_OUTPUT_PATH + '/' + prefix + '_720p_3000k.mp4')

	})
	.saveToFile(_OUTPUT_PATH + '/hls/720p_3000k.m3u8');
}

SD_480P_TRANSCODE = function(filename, prefix, callback) {
	_SD_480P = ffmpeg(filename, { presets: _PRESETS_PATH }).preset('hls')
	.videoBitrate(1500)
	.audioBitrate('128k')
	.size('854x480')
	.on('start', function(commandLine) {
	    wss.broadcast(JSON.stringify({'event': 'm3u8', 'status': 'start', 'rendition': '480P_1500K', 'command': commandLine}));
	})
	.on('progress', function(progress) {			
		progress['event'] = 'progress';
		progress['rendition'] = '480P_1500K';			
		wss.broadcast(JSON.stringify(progress));
	})
	.on('stderr', function(stderrLine) {
	    //console.log('Stderr output: ' + stderrLine);
	})
	.on('error', function(err, stdout, stderr) {
		wss.broadcast(JSON.stringify({'event': 'error', 'message': err.message}));
	})
	.on('end', function(stdout, stderr) {
	    wss.broadcast(JSON.stringify({'event': 'm3u8', 'status': 'complete', 'rendition': '480P_1500K'}));

	    ffmpeg(_OUTPUT_PATH + '/hls/480p_1500k.m3u8', { presets: _PRESETS_PATH }) // Concatenate M3U8 playlist into MP4 with moovatom at front
	    .preset('m3u8_to_mp4')
	   	.on('start', function(commandLine) {
	   		wss.broadcast(JSON.stringify({'event': 'mp4', 'status': 'start', 'rendition': '480P_1500K', 'command': commandLine}));
	   	})
	   	.on('end', function(stdout, stderr) {
   			wss.broadcast(JSON.stringify({'event': 'mp4', 'status': 'complete', 'rendition': '480P_1500K'}));
   		   
   		    return callback(null, prefix + '_480p_1500k.mp4');
	   	})
	   	.saveToFile(_OUTPUT_PATH + '/' + prefix + '_480p_1500k.mp4')

	})
	.saveToFile(_OUTPUT_PATH + '/hls/480p_1500k.m3u8');
}

SD_360P_TRANSCODE = function(filename, prefix, callback) {
	_SD_360P = ffmpeg(filename, { presets: _PRESETS_PATH }).preset('hls')
	.videoBitrate(850)
	.audioBitrate('128k')
	.size('640x360')
	.on('start', function(commandLine) {
	    wss.broadcast(JSON.stringify({'event': 'm3u8', 'status': 'start', 'rendition': '360P_850K', 'command': commandLine}));
	})
	.on('progress', function(progress) {			
		progress['event'] = 'progress';
		progress['rendition'] = '360P_850K';			
		wss.broadcast(JSON.stringify(progress));
	})
	.on('stderr', function(stderrLine) {
	    //console.log('Stderr output: ' + stderrLine);
	})
	.on('error', function(err, stdout, stderr) {
		wss.broadcast(JSON.stringify({'event': 'error', 'message': err.message}));
	})
	.on('end', function(stdout, stderr) {
	    wss.broadcast(JSON.stringify({'event': 'm3u8', 'status': 'complete', 'rendition': '360P_850K'}));

	    ffmpeg(_OUTPUT_PATH + '/hls/360p_850k.m3u8', { presets: _PRESETS_PATH }) // Concatenate M3U8 playlist into MP4 with moovatom at front
	    .preset('m3u8_to_mp4')
	   	.on('start', function(commandLine) {
	   		wss.broadcast(JSON.stringify({'event': 'mp4', 'status': 'start', 'rendition': '360P_850K', 'command': commandLine}));
	   	})
	   	.on('end', function(stdout, stderr) {
   			wss.broadcast(JSON.stringify({'event': 'mp4', 'status': 'complete', 'rendition': '360P_850K'}));
   		    
   		    return callback(null, prefix + '_360p_850k.mp4');
	   	})
	   	.saveToFile(_OUTPUT_PATH + '/' + prefix + '_360p_850k.mp4')

	})
	.saveToFile(_OUTPUT_PATH + '/hls/360p_850k.m3u8');
}

SD_240P_TRANSCODE = function(filename, prefix, callback) {
	_SD_240P = ffmpeg(filename, { presets: _PRESETS_PATH }).preset('hls')
	.videoBitrate(400)
	.audioBitrate('128k')
	.size('352x240')
	.on('start', function(commandLine) {
	    wss.broadcast(JSON.stringify({'event': 'm3u8', 'status': 'start', 'rendition': '240P_400K', 'command': commandLine}));
	})
	.on('progress', function(progress) {			
		progress['event'] = 'progress';
		progress['rendition'] = '240P_400K';			
		wss.broadcast(JSON.stringify(progress));
	})
	.on('stderr', function(stderrLine) {
	    //console.log('Stderr output: ' + stderrLine);
	})
	.on('error', function(err, stdout, stderr) {
		wss.broadcast(JSON.stringify({'event': 'error', 'message': err.message}));
	})
	.on('end', function(stdout, stderr) {
	    wss.broadcast(JSON.stringify({'event': 'm3u8', 'status': 'complete', 'rendition': '240P_400K'}));

	    ffmpeg(_OUTPUT_PATH + '/hls/240p_400k.m3u8', { presets: _PRESETS_PATH }) // Concatenate M3U8 playlist into MP4 with moovatom at front
	    .preset('m3u8_to_mp4')
	   	.on('start', function(commandLine) {
	   		wss.broadcast(JSON.stringify({'event': 'mp4', 'status': 'start', 'rendition': '240P_400K', 'command': commandLine}));
	   	})
	   	.on('error', function(err, stdout, stderr) {
	   		console.log(err.message);
	   	})
	   	.on('end', function(stdout, stderr) {
   			wss.broadcast(JSON.stringify({'event': 'mp4', 'status': 'complete', 'rendition': '240P_400K'}));
   		
   		    return callback(null, prefix + '_240p_400k.mp4');
	   	})
	   	.saveToFile(_OUTPUT_PATH + '/' + prefix + '_240p_400k.mp4')
	})
	.saveToFile(_OUTPUT_PATH + '/hls/240p_400k.m3u8');
}

AAC_128KBPS_HLS = function(filename, callback) {
	_AAC_128KBPS = ffmpeg(filename, { presets: _PRESETS_PATH })
	.noVideo()
	.audioBitrate('128k')
	.outputOptions('-c:a', 'aac')
	.outputOptions('-hls_time', '6')
	.outputOptions('-hls_list_size', '0')
	.outputOptions('-f', 'hls')
	.on('start', function(commandLine) {
	    wss.broadcast(JSON.stringify({'event': 'm3u8', 'status': 'start', 'rendition': 'AAC_128KBPS', 'command': commandLine}));
	})
	.on('progress', function(progress) {			
		progress['event'] = 'progress';
		progress['rendition'] = 'AAC_128KBPS';			
		wss.broadcast(JSON.stringify(progress));
	})
	.on('stderr', function(stderrLine) {
	    //console.log('Stderr output: ' + stderrLine);
	})
	.on('error', function(err, stdout, stderr) {
		wss.broadcast(JSON.stringify({'event': 'error', 'message': err.message}));
	})
	.on('end', function(stdout, stderr) {
		wss.broadcast(JSON.stringify({'event': 'M3U8', 'status': 'complete', 'rendition': 'AAC_128KBPS'}));
   		return callback(null, '/hls/128kbps_aac.m3u8');
	})
	.saveToFile(_OUTPUT_PATH + '/hls/128kbps_aac.m3u8');
}

AAC_128KBPS_M4A = function(filename, prefix, callback) {
	_AAC_128KBPS = ffmpeg(filename, { presets: _PRESETS_PATH })
	.noVideo()
	.audioBitrate('128k')
	.outputOptions('-c:a', 'aac')
	.on('start', function(commandLine) {
	    wss.broadcast(JSON.stringify({'event': 'aac', 'status': 'start', 'rendition': 'AAC_128KBPS', 'command': commandLine}));
	})
	.on('progress', function(progress) {			
		progress['event'] = 'progress';
		progress['rendition'] = 'AAC_128KBPS';			
		wss.broadcast(JSON.stringify(progress));
	})
	.on('stderr', function(stderrLine) {
	    //console.log('Stderr output: ' + stderrLine);
	})
	.on('error', function(err, stdout, stderr) {
		wss.broadcast(JSON.stringify({'event': 'error', 'message': err.message}));
	})
	.on('end', function(stdout, stderr) {
		wss.broadcast(JSON.stringify({'event': 'aac', 'status': 'complete', 'rendition': 'AAC_128KBPS'}));
   		return callback(null, prefix + '_128kbps_aac.m4a');
	})
	.saveToFile(_OUTPUT_PATH + '/' + prefix + '_128kbps_aac.m4a');
}

CREATE_THUMBNAILS = function(filename, callback) {
	_thumbnailFiles = [];

	ffmpeg(filename)
	  	.on('filenames', function(filenames) {
			wss.broadcast(JSON.stringify({'event': 'thumbnail', 'status': 'start', 'files': filenames}));
			_thumbnailFiles = filenames;
	  	})
		.on('end', function() {
			wss.broadcast(JSON.stringify({'event': 'thumbnail', 'status': 'complete'}));
			return callback(null, _thumbnailFiles);
		})
		.screenshots({
			count: 5, // Will take screens at 20%, 40%, 60% and 80% of the video
			folder: _OUTPUT_PATH + '/thumbs',
			filename: 'thumbnail_%s_%00i_%r.jpg',
			size: '640x360'
		});
}

CREATE_INDEX_M3U8 = function(callback) {
	fs.createReadStream('index.m3u8').pipe(fs.createWriteStream(_OUTPUT_PATH + '/hls/index.m3u8'));
	return callback(null, "/hls/index.m3u8");
}

// Recursively retry upload to GCS if fails
GCS_UPLOAD_RECURSIVE = function(file, options, callback) {
	dest_bucket.upload(file, options, function(err, gFileObj) {
		if(err) {
			console.log("File upload failed for " + file + ", trying again.");
			GCS_UPLOAD_RECURSIVE(file, options, callback); // retry if error
			return;
		}

		_ret = {'event': 'gcsupload', 'file': gFileObj.name};
		wss.broadcast(JSON.stringify(_ret));
		return callback();
	});
}

TRANSCODE_FILE_TO_HLS_AND_UPLOAD = function(filename, prefix, destination, _callback) {
	async.waterfall([
		function(callback) { // Clear output directories
			fs.emptyDir(_OUTPUT_PATH, err => { // Clear out output path of old m3u8 files
				if (err) return console.error(err);
				fs.mkdirSync(path.join(_OUTPUT_PATH, 'hls'));
				fs.mkdirSync(path.join(_OUTPUT_PATH, 'thumbs'));
				callback();
			});
		},
		function(callback) { // Run transcodes in parallel
			async.parallel([
				function(callback) { CREATE_THUMBNAILS(filename, callback); },
				function(callback) { HD_720P_TRANSCODE(filename, prefix, callback); },
				function(callback) { SD_480P_TRANSCODE(filename, prefix, callback); },
				function(callback) { SD_360P_TRANSCODE(filename, prefix, callback); },
				function(callback) { SD_240P_TRANSCODE(filename, prefix, callback); },
				function(callback) { AAC_128KBPS_M4A(filename, prefix, callback); },
				function(callback) { CREATE_INDEX_M3U8(callback); },
				function(callback) { AAC_128KBPS_HLS(filename, callback); }
			],
			function(err, results) {
				console.log(results);
				callback(null, results);
			});
		},
		function(filenames, callback) { // Upload to GCS
			 dir.files(_OUTPUT_PATH, function(err, files) {
			 	if (err) throw err;
			 	console.log('Files to upload: ' + files.length);

			 	async.eachOfLimit(files, 100, function(absolute_fn, key, callback) {
			 		if(!MIME_TYPE_LUT[path.extname(absolute_fn)]) return; // Only upload allowed extensions

			 		var _options = { // GCS options 
			 			resumable: false, // default is true for files >5MB
			 			validation: false, // Disable crc32/md5 checksum validation 
			 			destination: path.join(destination, path.relative(_OUTPUT_PATH, absolute_fn)),
			 			metadata: {
			 				contentType: MIME_TYPE_LUT[path.extname(absolute_fn)],
			 				cacheControl: 'public, max-age=31556926'
			 			}
			 		};

			 		GCS_UPLOAD_RECURSIVE(absolute_fn, _options, callback);
			 	},
			 	function(err) {
			 		console.log('Completed upload ' + files.length);
			 		_callback(null, filenames);
			 	});
			 });
		}
	]);
}

MASTER_GAME_FOOTAGE_HLS = function(filename, job, callback) {
	wss.broadcast(JSON.stringify({'event': 'mp4', 'status': 'pending', 'rendition': '240P_400K'}));
	wss.broadcast(JSON.stringify({'event': 'mp4', 'status': 'pending', 'rendition': '360P_850K'}));
	wss.broadcast(JSON.stringify({'event': 'mp4', 'status': 'pending', 'rendition': '480P_1500K'}));
	wss.broadcast(JSON.stringify({'event': 'mp4', 'status': 'pending', 'rendition': '720P_3000K'}));
	wss.broadcast(JSON.stringify({'event': 'download', 'status': 'start', 'file': filename}));
	_transcodeInProgress = true;

	uuid = uuidv4();
	_GCS_BASEPATH = path.join('game_footage', uuid);

	async.waterfall([
		function(callback) { // Download file
			if(fs.existsSync(path.join(_INPUT_PATH, filename))) { // If file already downloaded in local directory, use that instead of downloading again
				wss.broadcast(JSON.stringify({'event': 'download', 'status': 'complete', 'file': filename}));
				return callback();
			} else {
				bucket.file(filename).download({ destination: path.join(_INPUT_PATH, filename) }, function(err) {
					if(err) { // Error handling (bucket file not found in GCS)
						wss.broadcast(JSON.stringify({'event': 'download', 'status': 'error', 'message': err.code + ' ' + err.message}));
						return callback(err.code);
					}
					wss.broadcast(JSON.stringify({'event': 'download', 'status': 'complete', 'file': filename}));
					return callback();
				});
			}
		},
		function(callback) {
			TRANSCODE_FILE_TO_HLS_AND_UPLOAD(path.join(_INPUT_PATH, filename), uuid, _GCS_BASEPATH, callback);
		}
	], function(err, results) {
		async.parallel([
			function(callback) { // Update queue status to FINISHED
				console.log('PUTTING TO assets_transcode_queue');
				request.put(_API_HOST + '/v2/transcode/jobs/' + job.id + '/finished', function(error, response, body) { callback(); });
			},
			function(callback) { // Update assets_game_footage table with urls
				console.log("PUTTING TO assets_game_footage");
				_PUT_BODY = {
					hls_playlist_url: 'http://cdn-google.broadcast.cx/' + path.join(_GCS_BASEPATH, results[6]),
					mp4_240p: 'http://cdn-google.broadcast.cx/' + path.join(_GCS_BASEPATH, results[4]),
					mp4_360p: 'http://cdn-google.broadcast.cx/' + path.join(_GCS_BASEPATH, results[3]),
					mp4_480p: 'http://cdn-google.broadcast.cx/' + path.join(_GCS_BASEPATH, results[2]),
					mp4_720p: 'http://cdn-google.broadcast.cx/' + path.join(_GCS_BASEPATH, results[1]),
					thumbnails: JSON.stringify(results[0].map(function(e) {return 'http://cdn-google.broadcast.cx/' + path.join(_GCS_BASEPATH, e)}))
				};

				request.put({
					url: _API_HOST + '/v2/assets/game_footage/' + job.asset_game_footage_id,
					method: 'PUT',
					json: _PUT_BODY
				}, function(error, response, body) {
					callback();
				});
			}
		], function(err, response) {
			if(err) return request.put(_API_HOST + '/v2/transcode/jobs/' + job.id + '/error', function(error, response, body) { _transcodeInProgress = false; });
			_transcodeInProgress = false;
		});
	});

}





setInterval(function() {
	if(_transcodeInProgress) return;

	async.waterfall([
		function(callback) { // Get list of queued transcodes from the DB
			request(_API_HOST + '/v2/transcode/jobs', function(error, response, body) {
				if(error) return callback('Error connecting to queue.');
				if(JSON.parse(body).length == 0) return callback('No jobs in the queue.');

				callback(null, JSON.parse(body)[0]);
			});
		},
		function(job, callback) { // Immediately follow by setting job status to IN_PROGRESS
			request.put(_API_HOST + '/v2/transcode/jobs/' + job.id + '/start', function(error, response, body) {
				_transcodeInProgress = true;
				callback(null, job);
			});
		},
		function(job, callback) {
			MASTER_GAME_FOOTAGE_HLS(job.filename, job, callback);
			console.log(job);
		}
	], function(err, response) {
		if(err) return console.log(err);
		console.log('response');
	});

}, 2000);


server.listen(_PORT, function() {
	console.log('%s listening at %s', server.name, server.url);
});





