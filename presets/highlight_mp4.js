exports.load = function(ffmpeg) {
  ffmpeg
    .outputOptions('-preset', 'fast')
    .outputOptions('-profile:v', 'high')
    .outputOptions('-level', '4.0')
    .outputOptions('-movflags', '+faststart')
};