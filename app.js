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
var dest_bucket = gcs.bucket('cx-video-content');

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
	_API_HOST = 'http://api.broadcast.cx';
	_PORT = 8080; // 8080 forwarded to 80 with iptables rule
	_WS_PORT = 8081;

} else { // Running local on development
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

_transcodeInProgress = false;




server.post('/transcode/hls/:filename', hlsTranscode);
server.post('/transcode/highlights/:filename', highlights);
server.post('/transcode/highlightReel/:filename', highlightReel);


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
		_PUT_BODY['masterPlaylistUrl'] = 'https://storage.googleapis.com/cx-video-content/' + _GCS_BASEPATH + 'index.m3u8';

		request.put({uri: _API_HOST + '/videos/' + uuid, json: _PUT_BODY}, function(err, response, body) {
			if (err) return console.error(err);

			console.log(body);

			for(var index in _thumbnailFiles) {
				_thumbnailFiles[index]['thumbnailUrl'] = 'https://storage.googleapis.com/cx-video-content/' + _GCS_BASEPATH + _thumbnailFiles[index]['thumbnailUrl'];
				request.post({uri: _API_HOST + '/thumbnails/' + uuid, json: _thumbnailFiles[index]}, function(err, response, body) {
					if (err) return console.error(err);
					console.log(body);
				});
			}

			for(var index in _MP4Files) {
				_MP4Files[index]['videoUrl'] = 'https://storage.googleapis.com/cx-video-content/' + _GCS_BASEPATH + _MP4Files[index]['videoUrl'];
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
				
				req.body.highlights[x].videoUrl = 'https://storage.googleapis.com/cx-video-content/highlights/' + _gcsFilename;
				req.body.highlights[x].thumbnailUrl = 'https://storage.googleapis.com/cx-video-content/highlights/' + _gcsThumbnailFilename;
				
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


				_JSON_BODY = {

				};

				request.post({uri: 'http://127.0.0.1:' + _PORT + '/transcode/hls/' + _output_filename, json: req.body.api}, function(err, response, body) {
					if (err) return console.error(err);
					console.log(response);
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










server.listen(_PORT, function() {
	console.log('%s listening at %s', server.name, server.url);
});





