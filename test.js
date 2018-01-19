require('./index')('<source>', 'oss://<accessKeyId>:<accessKeySecret>@<bucketName>.<endpoint>/<prefix>');
require('./index')({
    source: '<source>',
    target: 'oss://<accessKeyId>:<accessKeySecret>@<bucketName>.<endpoint>/<prefix>',
    headers: {
        'Cache-Control': 'no-cache'
    }
});