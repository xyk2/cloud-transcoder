var fs = require('fs-extra');
var restify = require('restify');
var ffmpeg = require('fluent-ffmpeg');
const WebSocket = require('ws');
var dir = require('node-dir');
var path = require('path');
var request = require('request');
var async = require('async');
const uuidv4 = require('uuid/v4')
var m3u8Parser = require('m3u8-parser');
var Raven = require('raven');

Raven.config('https://c790451322a743ea89955afd471c2985:2b9c188e39444388a717d04b73b3ad5f@sentry.io/249073').install();

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


if(process.env.NODE_ENV == 'production') {
	_INPUT_PATH = process.env.PWD + '/_inputs';
	_OUTPUT_PATH = process.env.PWD + '/_outputs';
	_PRESETS_PATH = process.env.PWD + '/presets';
	_API_HOST = 'http://api.broadcast.cx';
	_PORT = 8080; // 8080 forwarded to 80 with iptables rule
	_WS_PORT = 8081;

} else { // Running local on development
	_INPUT_PATH = '/Users/XYK/Desktop/Dropbox/broadcast.cx-ffmpeg-runner/_inputs';
	_OUTPUT_PATH = '/Users/XYK/Desktop/Dropbox/broadcast.cx-ffmpeg-runner/_outputs';
	_PRESETS_PATH = '/Users/XYK/Desktop/Dropbox/broadcast.cx-ffmpeg-runner/presets';
	_API_HOST = 'http://local.broadcast.cx:8087';
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




HD_720P_TRANSCODE = function(filename, prefix, startTime, endTime, callback) {
	_HD_720P = ffmpeg(filename, { presets: _PRESETS_PATH }).preset('hls')
	.videoBitrate(3000)
	.audioBitrate('128k')
	.renice(-10);

	if(startTime && endTime) {
		_HD_720P.seekInput(startTime-10 < 0 ? 0 : startTime - 10);
		_HD_720P.outputOptions(['-ss ' + (startTime).toFixed(2), '-t ' + (endTime - startTime), '-copyts']);
	}

	if(path.extname(filename) == '.avi') {
		_HD_720P.outputOptions('-filter:v', "yadif=0, scale=w=1280:h=720'");
		_HD_720P.outputOptions('-pix_fmt', 'yuv420p');
	}
	else _HD_720P.outputOptions('-filter:v', 'scale=w=1280:h=720');

	_HD_720P.on('start', function(commandLine) {
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
		return callback(err.message);
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

SD_480P_TRANSCODE = function(filename, prefix, startTime, endTime, callback) {
	_SD_480P = ffmpeg(filename, { presets: _PRESETS_PATH }).preset('hls')
	.videoBitrate(1500)
	.audioBitrate('128k')
	.renice(-10);

	if(startTime && endTime) {
		_SD_480P.seekInput(startTime-10 < 0 ? 0 : startTime - 10);
		_SD_480P.outputOptions(['-ss ' + (startTime).toFixed(2), '-t ' + (endTime - startTime), '-copyts']);
	}

	if(path.extname(filename) == '.avi') {
		_SD_480P.outputOptions('-filter:v', "yadif=0, scale=w=854:h=480'");
		_SD_480P.outputOptions('-pix_fmt', 'yuv420p');
	}
	else _SD_480P.outputOptions('-filter:v', 'scale=w=854:h=480');


	_SD_480P.on('start', function(commandLine) {
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
		return callback(err.message);
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

SD_360P_TRANSCODE = function(filename, prefix, startTime, endTime, callback) {
	_SD_360P = ffmpeg(filename, { presets: _PRESETS_PATH }).preset('hls')
	.videoBitrate(850)
	.audioBitrate('128k')
	.renice(-10);

	if(startTime && endTime) {
		_SD_360P.seekInput(startTime-10 < 0 ? 0 : startTime - 10);
		_SD_360P.outputOptions(['-ss ' + (startTime).toFixed(2), '-t ' + (endTime - startTime), '-copyts']);
	}

	if(path.extname(filename) == '.avi') {
		_SD_360P.outputOptions('-filter:v', "yadif=0, scale=w=640:h=360'");
		_SD_360P.outputOptions('-pix_fmt', 'yuv420p');
	}
	else _SD_360P.outputOptions('-filter:v', 'scale=w=640:h=360');


	_SD_360P.on('start', function(commandLine) {
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
		return callback(err.message);
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

SD_240P_TRANSCODE = function(filename, prefix, startTime, endTime, callback) {
	_SD_240P = ffmpeg(filename, { presets: _PRESETS_PATH }).preset('hls')
	.videoBitrate(400)
	.audioBitrate('128k')
	.renice(-10);

	if(startTime && endTime) {
		_SD_240P.seekInput(startTime-10 < 0 ? 0 : startTime - 10);
		_SD_240P.outputOptions(['-ss ' + (startTime).toFixed(2), '-t ' + (endTime - startTime), '-copyts']);
	}

	if(path.extname(filename) == '.avi') {
		_SD_240P.outputOptions('-filter:v', "yadif=0, scale=w=352:h=240'");
		_SD_240P.outputOptions('-pix_fmt', 'yuv420p');
	}
	else _SD_240P.outputOptions('-filter:v', 'scale=w=352:h=240');


	_SD_240P.on('start', function(commandLine) {
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
		return callback(err.message);
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

AAC_128KBPS_HLS = function(filename, startTime, endTime, callback) {
	_AAC_128KBPS = ffmpeg(filename, { presets: _PRESETS_PATH })
	.noVideo()
	.audioBitrate('128k')
	.outputOptions('-c:a', 'aac')
	.outputOptions('-hls_time', '6')
	.outputOptions('-hls_list_size', '0')
	.outputOptions('-f', 'hls');

	if(startTime && endTime) {
		_AAC_128KBPS.seekInput(startTime-10 < 0 ? 0 : startTime - 10);
		_AAC_128KBPS.outputOptions(['-ss ' + (startTime).toFixed(2), '-t ' + (endTime - startTime), '-copyts']);
	}

	_AAC_128KBPS.on('start', function(commandLine) {
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
		return callback(err.message);
	})
	.on('end', function(stdout, stderr) {
		wss.broadcast(JSON.stringify({'event': 'M3U8', 'status': 'complete', 'rendition': 'AAC_128KBPS'}));
   		return callback(null, '/hls/128kbps_aac.m3u8');
	})
	.saveToFile(_OUTPUT_PATH + '/hls/128kbps_aac.m3u8');
}

AAC_128KBPS_M4A = function(filename, prefix, startTime, endTime, callback) {
	_AAC_128KBPS = ffmpeg(filename, { presets: _PRESETS_PATH })
	.noVideo()
	.audioBitrate('128k')
	.outputOptions('-c:a', 'aac');

	if(startTime && endTime) {
		_AAC_128KBPS.seekInput(startTime-10 < 0 ? 0 : startTime - 10);
		_AAC_128KBPS.outputOptions(['-ss ' + (startTime).toFixed(2), '-t ' + (endTime - startTime), '-copyts']);
	}

	_AAC_128KBPS.on('start', function(commandLine) {
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
		return callback(err.message);
	})
	.on('end', function(stdout, stderr) {
		wss.broadcast(JSON.stringify({'event': 'aac', 'status': 'complete', 'rendition': 'AAC_128KBPS'}));
   		return callback(null, prefix + '_128kbps_aac.m4a');
	})
	.saveToFile(_OUTPUT_PATH + '/' + prefix + '_128kbps_aac.m4a');
}

CREATE_THUMBNAILS = function(filename, startTime, endTime, callback) {
	_thumbnailFiles = [];

	_THUMBNAILS = ffmpeg(filename);
	_OPTIONS = {
		count: 5, // Will take screens at 20%, 40%, 60% and 80% of the video
		folder: _OUTPUT_PATH + '/thumbs',
		filename: 'thumbnail_%s_%00i_%r.jpg',
		size: '640x360'
	};

	if(startTime && endTime) { // if start/end time specified, take screenshots in between startTime and endTime
		_OPTIONS.timemarks = [];
		_OPTIONS.timemarks.push(startTime);
		_OPTIONS.timemarks.push(startTime + ((endTime - startTime) / 3) * 1);
		_OPTIONS.timemarks.push(startTime + ((endTime - startTime) / 3) * 2);
		_OPTIONS.timemarks.push(startTime + ((endTime - startTime) / 3) * 3);
		_OPTIONS.timemarks.push(endTime);
	}

  	_THUMBNAILS.on('filenames', function(filenames) {
		wss.broadcast(JSON.stringify({'event': 'thumbnail', 'status': 'start', 'files': filenames}));
		_thumbnailFiles = filenames;
  	})
	.on('end', function() {
		wss.broadcast(JSON.stringify({'event': 'thumbnail', 'status': 'complete'}));
		return callback(null, _thumbnailFiles);
	})
	.on('error', function(err, stdout, stderr) {
		wss.broadcast(JSON.stringify({'event': 'error', 'message': err.message}));
		return callback(err.message);
	})
	.screenshots(_OPTIONS);
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
			Raven.captureException(err);
			GCS_UPLOAD_RECURSIVE(file, options, callback); // retry if error
			return;
		}

		_ret = {'event': 'gcsupload', 'file': gFileObj.name};
		wss.broadcast(JSON.stringify(_ret));
		return callback();
	});
}

TRANSCODE_FILE_TO_HLS_AND_UPLOAD = function(filename, prefix, startTime, endTime, destination, _callback) {
	async.waterfall([
		function(callback) { // Clear output directories
			fs.emptyDir(_OUTPUT_PATH, err => { // Clear out output path of old m3u8 files
				if (err) return _callback(err);
				fs.mkdirSync(path.join(_OUTPUT_PATH, 'hls'));
				fs.mkdirSync(path.join(_OUTPUT_PATH, 'thumbs'));
				callback();
			});
		},
		function(callback) { // Run transcodes in parallel
			async.parallel([ // ORDER MATTERS HERE
				function(callback) { CREATE_THUMBNAILS(filename, startTime, endTime, callback); },
				function(callback) { HD_720P_TRANSCODE(filename, prefix, startTime, endTime, callback); },
				function(callback) { SD_480P_TRANSCODE(filename, prefix, startTime, endTime, callback); },
				function(callback) { SD_360P_TRANSCODE(filename, prefix, startTime, endTime, callback); },
				function(callback) { SD_240P_TRANSCODE(filename, prefix, startTime, endTime, callback); },
				function(callback) { AAC_128KBPS_M4A(filename, prefix, startTime, endTime, callback); },
				function(callback) { CREATE_INDEX_M3U8(callback); },
				function(callback) { AAC_128KBPS_HLS(filename, startTime, endTime, callback); }
			],
			function(err, results) {
				if(err) return _callback(err);

				// Get total duration of video from parsing output m3u8
				var parser = new m3u8Parser.Parser();
				parser.push(fs.readFileSync(path.join(_OUTPUT_PATH, results[6].replace('index', '720p_3000k')), 'utf-8'));
				parser.end();
				var parsed_mfst = parser.manifest;

				_total_duration = 0;
				for (segment in parsed_mfst.segments) _total_duration += parsed_mfst.segments[segment].duration;

				_ret = {
					filenames: results,
					duration: parseInt(_total_duration * 1000)
				};

				console.log(_ret);
				callback(null, _ret);
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
		function(callback) { // Clear input directory if running on prod
			if(process.env.NODE_ENV != 'production') return callback();
			fs.emptyDir(_INPUT_PATH, err => { // Clear out input path so storage doesn't fill up on instance
				if (err) return callback(err);
				return callback();
			});
		},
		function(callback) { // Download file
			if(fs.existsSync(path.join(_INPUT_PATH, filename))) { // If file already downloaded in local directory, use that instead of downloading again
				wss.broadcast(JSON.stringify({'event': 'download', 'status': 'complete', 'file': filename}));
				return callback();
			} else { // TODO: upgrade to aria2c, significantly faster
				bucket.file(filename).download({ destination: path.join(_INPUT_PATH, filename) }, function(err) {
					if(err) { // Error handling (bucket file not found in GCS)
						wss.broadcast(JSON.stringify({'event': 'download', 'status': 'error', 'message': err.code + ' ' + err.message}));
						return callback(err.code + ' ' + err.message);
					}
					wss.broadcast(JSON.stringify({'event': 'download', 'status': 'complete', 'file': filename}));
					return callback();
				});
			}
		},
		function(callback) {
			TRANSCODE_FILE_TO_HLS_AND_UPLOAD(path.join(_INPUT_PATH, filename), uuid, null, null, _GCS_BASEPATH, callback);
		}
	], function(err, results) {
		if(err) {
			Raven.captureException(err);
			return request.put({
				url: _API_HOST + '/v2/transcode/jobs/' + job.id + '/error',
				method: 'PUT',
				json: { message: err }
			}, function(error, response, body) {
				_transcodeInProgress = false;
				console.log('PUTTING TO assets_transcode_queue: ERROR')
			});
		}

		async.parallel([
			function(callback) { // Update queue status to FINISHED
				console.log('PUTTING TO assets_transcode_queue: FINISHED');
				request.put(_API_HOST + '/v2/transcode/jobs/' + job.id + '/finished', function(error, response, body) { callback(); });
			},
			function(callback) { // Update assets_game_footage table with urls
				console.log("PUTTING TO assets_game_footage " + '/v2/assets/game_footage/' + job.asset_game_footage_id);
				_PUT_BODY = {
					hls_playlist_url: 'https://cdn-google.broadcast.cx/' + path.join(_GCS_BASEPATH, results.filenames[6]),
					mp4_240p: 'https://cdn-google.broadcast.cx/' + path.join(_GCS_BASEPATH, results.filenames[4]),
					mp4_360p: 'https://cdn-google.broadcast.cx/' + path.join(_GCS_BASEPATH, results.filenames[3]),
					mp4_480p: 'https://cdn-google.broadcast.cx/' + path.join(_GCS_BASEPATH, results.filenames[2]),
					mp4_720p: 'https://cdn-google.broadcast.cx/' + path.join(_GCS_BASEPATH, results.filenames[1]),
					thumbnails: JSON.stringify(results.filenames[0].map(function(e) {return 'https://cdn-google.broadcast.cx/' + path.join(_GCS_BASEPATH, 'thumbs', e)})),
					duration: results.duration
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
			_transcodeInProgress = false;
		});
	});

}

MASTER_TRIM = function(filename, job, callback) {
	_transcodeInProgress = true;
	uuid = uuidv4();

	_GCS_BASEPATH = path.join('events', uuid);

	async.waterfall([
		function(callback) {
			if(!job.parameters || !('startTime' in job.parameters) || !('endTime' in job.parameters) || !job.parameters.api) {
				return callback("Incorrect / missing information in parameter.");
			} else {
				callback();
			}
		},
		function(callback) { // Clear input directory if running on prod
			if(process.env.NODE_ENV != 'production') return callback();
			fs.emptyDir(_INPUT_PATH, err => { // Clear out input path so storage doesn't fill up on instance
				if (err) return callback(err);
				return callback();
			});
		},
		function(callback) { // Download file
			if(fs.existsSync(path.join(_INPUT_PATH, filename))) { // If file already downloaded in local directory, use that instead of downloading again
				wss.broadcast(JSON.stringify({'event': 'download', 'status': 'complete', 'file': filename}));
				return callback();
			} else { // TODO: upgrade to aria2c, significantly faster
				bucket.file(filename).download({ destination: path.join(_INPUT_PATH, filename) }, function(err) {
					if(err) { // Error handling (bucket file not found in GCS)
						wss.broadcast(JSON.stringify({'event': 'download', 'status': 'error', 'message': err.code + ' ' + err.message}));
						return callback(err.code + ' ' + err.message);
					}
					wss.broadcast(JSON.stringify({'event': 'download', 'status': 'complete', 'file': filename}));
					return callback();
				});
			}
		},
		function(callback) {
			TRANSCODE_FILE_TO_HLS_AND_UPLOAD(path.join(_INPUT_PATH, filename), uuid, job.parameters.startTime, job.parameters.endTime, _GCS_BASEPATH, callback);
		}
	], function(err, results) {
		if(err) {
			Raven.captureException(err);
			return request.put({
				url: _API_HOST + '/v2/transcode/jobs/' + job.id + '/error',
				method: 'PUT',
				json: { message: err }
			}, function(error, response, body) {
				_transcodeInProgress = false;
				console.log('PUTTING TO assets_transcode_queue: ERROR')
			});
		}

		async.parallel([
			function(callback) { // Update queue status to FINISHED
				console.log('PUTTING TO assets_transcode_queue: FINISHED');
				request.put(_API_HOST + '/v2/transcode/jobs/' + job.id + '/finished', function(error, response, body) { callback(); });
			},
			function(callback) { // Update v2_videos table with urls
				console.log("POST TO v2_videos");
				_POST_BODY = {
					hls_playlist_url: 'https://cdn-google.broadcast.cx/' + path.join(_GCS_BASEPATH, results.filenames[6]),
					mp4_240p: 'https://cdn-google.broadcast.cx/' + path.join(_GCS_BASEPATH, results.filenames[4]),
					mp4_360p: 'https://cdn-google.broadcast.cx/' + path.join(_GCS_BASEPATH, results.filenames[3]),
					mp4_480p: 'https://cdn-google.broadcast.cx/' + path.join(_GCS_BASEPATH, results.filenames[2]),
					mp4_720p: 'https://cdn-google.broadcast.cx/' + path.join(_GCS_BASEPATH, results.filenames[1]),
					thumbnails: JSON.stringify(results.filenames[0].map(function(e) {return 'https://cdn-google.broadcast.cx/' + path.join(_GCS_BASEPATH, 'thumbs', e)})),
					duration: results.duration,
					source_game_footage_id: job.asset_game_footage_id,
					uuid: uuid
				};

				_POST_BODY = Object.assign(_POST_BODY, job.parameters.api);
				console.log(_POST_BODY);

				request.post({
					url: _API_HOST + '/v2/videos',
					method: 'POST',
					json: _POST_BODY
				}, function(error, response, body) {
					callback();
				});
			}
		], function(err, response) {
			_transcodeInProgress = false;
		});
	});
}





setInterval(function() { // Poll DB for new jobs if there is no transcode in progress
	if(_transcodeInProgress) return;

	async.waterfall([
		function(callback) { // Get list of queued transcodes from the DB
			request.put(_API_HOST + '/v2/transcode/jobs/request', function(error, response, body) {
				if(error) return callback('Error connecting to queue.');
				if(JSON.parse(body).length == 0) return callback('No jobs in the queue.');

				callback(null, JSON.parse(body));
			});
		},
		function(job, callback) {
			_transcodeInProgress = true;

			if(job.type == 'fullGame') MASTER_GAME_FOOTAGE_HLS(job.filename, job, callback);
			if(job.type == 'event') MASTER_TRIM(job.filename, job, callback);
		}
	], function(err, response) {
		if(err) return console.log(err);
	});
}, 2000 + Math.floor(Math.random() * 1000)); // Keep initialization times somewhat random


server.listen(_PORT, function() {
	console.log('%s listening at %s', server.name, server.url);
});





