exports.load = function(ffmpeg) {
  ffmpeg
    .outputOptions('-preset', 'veryfast')
    .outputOptions('-profile:v', 'high')
    .outputOptions('-level', '4.0')
    .outputOptions('-movflags', '+faststart')
};