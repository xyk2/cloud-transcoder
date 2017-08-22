exports.load = function(ffmpeg) {
  ffmpeg
    .outputOptions('-c', 'copy')
    .outputOptions('-bsf:a', 'aac_adtstoasc')
    .outputOptions('-movflags', '+faststart')
};