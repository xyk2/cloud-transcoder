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

var MIME_TYPE_LUT = {
	'.m3u8': 'application/x-mpegURL',
	'.ts': 'video/MP2T',
	'.jpg': 'image/jpeg',
	'.mp4': 'video/mp4',
	'.m4a': 'audio/mp4'
}

if(process.env.NODE_ENV == 'production') {
	_INPUT_PATH = process.env.PWD + '/_inputs';
	_OUTPUT_PATH = process.env.PWD + '/_outputs';
	_PRESETS_PATH = process.env.PWD + '/presets';
	_API_HOST = 'http://api.broadcast.cx';
	_PORT = 8080; // 8080 forwarded to 80 with iptables rule
	_WS_PORT = 8081;
	Raven.config('https://c790451322a743ea89955afd471c2985:2b9c188e39444388a717d04b73b3ad5f@sentry.io/249073').install();

} else { // Running local on development
	_INPUT_PATH = '/Users/XYK/Desktop/Dropbox/broadcast.cx-ffmpeg-runner/_inputs';
	_OUTPUT_PATH = '/Users/XYK/Desktop/Dropbox/broadcast.cx-ffmpeg-runner/_outputs';
	_PRESETS_PATH = '/Users/XYK/Desktop/Dropbox/broadcast.cx-ffmpeg-runner/presets';
	_API_HOST = 'http://local.broadcast.cx:8087';
	ffmpeg.setFfmpegPath('/Users/XYK/Desktop/ffmpeg'); // Explicitly set ffmpeg and ffprobe paths
	ffmpeg.setFfprobePath('/Users/XYK/Desktop/ffprobe');
	_PORT = 8082;
	_WS_PORT = 8081;
}

// Grab machine details from GCP if available, otherwise null
var machine_details = {};

request({
	url: 'http://metadata.google.internal/computeMetadata/v1/instance/?recursive=true',
	headers: { 'Metadata-Flavor': 'Google' },
	timeout: 1500
}, function(error, response, body) {
	if(error) return; // If times out or no response at all
	else machine_details = JSON.parse(body);
});

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

server.get('/healthcheck', function(req, res) {
	res.send(machine_details);
});

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
				var _download_start_time = Date.now();

				bucket.file(filename).download({ destination: path.join(_INPUT_PATH, filename) }, function(err) {
					if(err) { // Error handling (bucket file not found in GCS)
						wss.broadcast(JSON.stringify({'event': 'download', 'status': 'error', 'message': err.code + ' ' + err.message}));
						return callback(err.code + ' ' + err.message);
					}

					request.put({ url: `${_API_HOST}/v2/transcode/jobs/${job.id}/download_meta`, json: { download_time: parseInt(Date.now() - _download_start_time), download_method: 'google-cloud' }}, function(error, response, body) { });

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
				var _download_start_time = Date.now();

				bucket.file(filename).download({ destination: path.join(_INPUT_PATH, filename) }, function(err) {
					if(err) { // Error handling (bucket file not found in GCS)
						wss.broadcast(JSON.stringify({'event': 'download', 'status': 'error', 'message': err.code + ' ' + err.message}));
						return callback(err.code + ' ' + err.message);
					}

					request.put({ url: `${_API_HOST}/v2/transcode/jobs/${job.id}/download_meta`, json: { download_time: parseInt(Date.now() - _download_start_time), download_method: 'google-cloud' }}, function(error, response, body) { });

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


/*
{
	source_asset_id: 1212,
	startTime: 10.0,
	endTime: 19.1,
	user_id: 1868,
	title: 'LOL',
	description: 'suckonthese'
}
*/

MASTER_INDIVIDUAL_HIGHLIGHT = function(filename, job, callback) {
	wss.broadcast(JSON.stringify({'event': 'mp4', 'status': 'pending', 'rendition': '720P_2000K'}));
	wss.broadcast(JSON.stringify({'event': 'download', 'status': 'start', 'file': filename}));

	_transcodeInProgress = true;

	uuid = uuidv4();

	_GCS_BASEPATH = path.join('individual_highlight', uuid);

	async.waterfall([
		// Check for correct parameters in the job
		function(callback) {
			if(!job.parameters || !('startTime' in job.parameters) || !('endTime' in job.parameters)) {
				return callback("Incorrect / missing information in parameter.");
			} else {
				callback();
			}
		},
		// Clear input directory if running on prod
		function(callback) {
			if(process.env.NODE_ENV != 'production') return callback();
			fs.emptyDir(_INPUT_PATH, err => { // Clear out input path so storage doesn't fill up on instance
				if (err) return callback(err);
				return callback();
			});
		},
		// Download file if it doesn't exist
		function(callback) {
			if(fs.existsSync(path.join(_INPUT_PATH, filename))) { // If file already downloaded in local directory, use that instead of downloading again
				wss.broadcast(JSON.stringify({'event': 'download', 'status': 'complete', 'file': filename}));
				return callback();

			} else {
				// TODO: upgrade to aria2c, significantly faster
				// Log download times

				var _download_start_time = Date.now();

				bucket.file(filename).download({ destination: path.join(_INPUT_PATH, filename) }, function(err) {
					if(err) { // Error handling (bucket file not found in GCS)
						wss.broadcast(JSON.stringify({'event': 'download', 'status': 'error', 'message': err.code + ' ' + err.message}));
						return callback(err.code + ' ' + err.message);
					}

					request.put({ url: `${_API_HOST}/v2/transcode/jobs/${job.id}/download_meta`, json: { download_time: parseInt(Date.now() - _download_start_time), download_method: 'google-cloud' }}, function(error, response, body) { });

					wss.broadcast(JSON.stringify({'event': 'download', 'status': 'complete', 'file': filename}));
					return callback();
				});
			}
		},
		// Clear output directories
		function(callback) {
			fs.emptyDir(_OUTPUT_PATH, err => { // Clear out output path of old m3u8 files
				if (err) return callback(err);
				fs.mkdirSync(path.join(_OUTPUT_PATH, 'thumbs'));
				callback();
			});
		},
		function(callback) {
			async.parallel([
				(callback) => {
					_HD_720P = ffmpeg(path.join(_INPUT_PATH, filename), { presets: _PRESETS_PATH }).preset('highlight_mp4')
					.videoBitrate(2000)
					.audioBitrate('96k')
					.renice(-10);

					if(job.parameters.startTime && job.parameters.endTime) {
						job.parameters.startTime = Math.max(job.parameters.startTime, 0);
						_HD_720P.seekInput(job.parameters.startTime - 10 < 0 ? 0 : job.parameters.startTime - 10);
						_HD_720P.outputOptions(['-ss ' + (job.parameters.startTime).toFixed(2), '-t ' + (job.parameters.endTime - job.parameters.startTime), '-copyts']);
					}

					if(path.extname(filename) == '.avi') {
						_HD_720P.outputOptions('-filter:v', "yadif=0, scale=w=1280:h=720'");
						_HD_720P.outputOptions('-pix_fmt', 'yuv420p');
					}
					else _HD_720P.outputOptions('-filter:v', 'scale=w=1280:h=720');

					_HD_720P.on('start', function(commandLine) {
					    wss.broadcast(JSON.stringify({'event': 'm3u8', 'status': 'start', 'rendition': '720P_2000K', 'command': commandLine}));
					})
					.on('progress', function(progress) {
						progress['event'] = 'progress';
						progress['rendition'] = '720P_2000K';
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
					    wss.broadcast(JSON.stringify({'event': 'm3u8', 'status': 'complete', 'rendition': '720P_2000K'}));

				   		return callback(null, uuid + '_720p_3000k.mp4');
					})
					.saveToFile(_OUTPUT_PATH + '/' + uuid + '_720p_3000k.mp4');
				},
				(callback) => {
					CREATE_THUMBNAILS(path.join(_INPUT_PATH, filename), job.parameters.startTime, job.parameters.endTime, callback);
				}
			], (err, results) => {
				if(err) return callback(err);
				return callback(null, results);
			});
		},
		// Upload to GCS
		function(filenames, callback) {
			dir.files(_OUTPUT_PATH, function(err, files) {
				if(err) throw err;
				console.log('Files to upload: ' + files.length);

				async.eachOfLimit(files, 100, function(absolute_fn, key, callback) {
					if(!MIME_TYPE_LUT[path.extname(absolute_fn)]) return; // Only upload allowed extensions

					var _options = { // GCS options
						resumable: false, // default is true for files >5MB
						validation: false, // Disable crc32/md5 checksum validation
						destination: path.join(_GCS_BASEPATH, path.relative(_OUTPUT_PATH, absolute_fn)),
						metadata: {
							contentType: MIME_TYPE_LUT[path.extname(absolute_fn)],
							cacheControl: 'public, max-age=31556926'
						}
					};

					GCS_UPLOAD_RECURSIVE(absolute_fn, _options, callback);
				},
				function(err) {
					console.log('Completed upload ' + files.length);
					callback(null, filenames);
				});
			});
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
			function(callback) { // Update user_exported_highlights table with urls
				console.log("PUT TO user_exported_highlights");

				_PUT_BODY = {
					mp4_720p: 'https://cdn-google.broadcast.cx/' + path.join(_GCS_BASEPATH, results[0]),
					thumbnails: JSON.stringify(results[1].map(function(e) {return 'https://cdn-google.broadcast.cx/' + path.join(_GCS_BASEPATH, 'thumbs', e)})),
					duration: parseInt((job.parameters.endTime - job.parameters.startTime) * 1000)
				};


				request.put({
					url: _API_HOST + '/v2/assets/individual_highlight/' + job.parameters.individual_highlight_id,
					method: 'PUT',
					json: _PUT_BODY
				}, function(error, response, body) { callback(); });
			}
		], function(err, response) {
			_transcodeInProgress = false;
		});
	});

}





setInterval(function() { // Poll DB for new jobs if there is no transcode in progress
	if(_transcodeInProgress) return;

	async.waterfall([
		function(callback) {
			// Get list of queued transcodes from the DB
			// Send machine information along
			request.put({
				url: _API_HOST + '/v2/transcode/jobs/request',
				method: 'PUT',
				json: {
					machine_details: machine_details // Send machine information
				}
			}, function(error, response, body) {
				if(error) return callback('Error connecting to queue.');
				if(body.length == 0) return callback('No jobs in the queue.');

				callback(null, body);
			});
		},
		function(job, callback) {
			_transcodeInProgress = true;

			if(job.type == 'fullGame') MASTER_GAME_FOOTAGE_HLS(job.filename, job, callback);
			if(job.type == 'event') MASTER_TRIM(job.filename, job, callback);
			if(job.type == 'individualHighlight') MASTER_INDIVIDUAL_HIGHLIGHT(job.filename, job, callback);
		}
	], function(err, response) {
		if(err) return console.log(err);
	});
}, 2000 + Math.floor(Math.random() * 1000)); // Keep initialization times somewhat random


server.listen(_PORT, function() {
	console.log('%s listening at %s', server.name, server.url);
});





