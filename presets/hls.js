exports.load = function(ffmpeg) {
  ffmpeg
    .outputOptions('-c:a', 'aac')
    .outputOptions('-preset', 'veryfast')
    .outputOptions('-x264-params', 'keyint=60:min-keyint=60:scenecut=-1')
    .outputOptions('-profile:v', 'high')
    .outputOptions('-level', '4.0')
    //.outputOptions('-ac', '2')
    .outputOptions('-movflags', '+faststart')
    .outputOptions('-start_number', '0')
    .outputOptions('-hls_time', '6')
    .outputOptions('-hls_list_size', '0')
    .outputOptions('-max_muxing_queue_size', '1024') // Some MP4s coming from Wowza error out on start because buffer is too small
    .outputOptions('-f', 'hls');
};