var restify = require('restify');
var ffmpeg = require('fluent-ffmpeg');


var server = restify.createServer({
	name: 'ffmpeg-runner'
});

ffmpeg.setFfmpegPath('/Users/XYK/Desktop/ffmpeg'); // Explicitly set ffmpeg and ffprobe paths


server.get('/transcode/:filename', transcode);

_HD_720P = {};

console.log(process.hrtime()[0]);

function transcode(req, res, next) {

	_HD_720P[process.hrtime()[0]] = ffmpeg(req.params.filename, {"stdoutLines": 0})
	.videoBitrate(3000)
	.audioBitrate('96k')
	.size('1280x720')
	.save('output.mp4')
	.on('start', function(commandLine) {
		console.log('FFMPEG: ' + commandLine);
	})
	.on('progress', function(progress) {
		console.log('Processing: ' + JSON.stringify(progress));
	})
	.on('stderr', function(stderrLine) {
	    console.log('Stderr output: ' + stderrLine);
	})
	.on('error', function(err, stdout, stderr) {
	    console.log(err, stdout, stderr);
	    console.log('Cannot process video: ' + err.message);
	    console.log('Cannot process video: ' + err.message);
	})
	.on('end', function(stdout, stderr) {
	    console.log(stdout);
	    console.log(stderr);
	    console.log('Transcoding successful.');

	});
}



server.listen(8080, function() {
	console.log('%s listening at %s', server.name, server.url);
});
