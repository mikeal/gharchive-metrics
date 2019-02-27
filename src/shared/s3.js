const AWS = require('aws-sdk')
const { Transform, PassThrough } = require('stream')
const jsonstream = require('jsonstream2')
const awsConfig = require('aws-config')
const UploadStream = require('s3-stream-upload')
const downloader = require('s3-download-stream')
const RecursiveIterator = require('recursive-iterator')
const recursive = arg => new RecursiveIterator(arg)

const { promisify } = require('util')

module.exports = (profile, bucketName = 'ipfs-metrics') => {
  var s3 = new AWS.S3(awsConfig({ profile, sslEnabled: true }))

  const Bucket = bucketName

  const upload = key => UploadStream(s3, { Bucket, Key: key })

  const waitForStream = stream => {
    return new Promise((resolve, reject) => {
      stream.on('error', reject)
      stream.on('finish', resolve)
    })
  }

  const hasObject = key => {
    return new Promise((resolve, reject) => {
      s3.headObject({Bucket, Key: key}, function (err, bool) {
        if (err) return resolve(false)
        resolve(bool ? {size: parseInt(this.httpResponse.headers['content-length'])} : bool)
      })
    })
  }

  const getStream = key => {
    var config = {
      client: s3,
      concurrency: 6,
      params: {
        Key: key,
        Bucket
      }
    }
    return downloader(config)
  }

  const getObject = key => {
    return new Promise((resolve, reject) => {
      let stream = getStream(key)
      let parts = []
      stream.on('data', chunk => parts.push(chunk))
      stream.on('error', reject)
      stream.on('end', () => {
        resolve(Buffer.concat(parts))
      })
    })
  }

  const putObject = (key, data) => {
    return new Promise((resolve, reject) => {
      s3.putObject({
        Bucket,
        Key: key,
        Body: data,
        ACL: 'public-read'
      }, (err, r) => {
        if (err) return reject(err)
        resolve(r)
      })
    })
  }
  const storeBlock = async block => {
    return putObject(`blocks/${block.cid.toBaseEncodedString()}`, block.data)
  }


  const query = (expression, key) => {
    const params = {
      Bucket,
      Key: key,
      ExpressionType: 'SQL',
      Expression: expression,
      InputSerialization: {
        JSON: {
          Type: 'LINES'
        },
        CompressionType: 'GZIP'
      },
      OutputSerialization: {
        JSON: {
        }
      }
    }
    return new Promise((resolve, reject) => {
      s3.selectObjectContent(params, (err, data) => {
        if (err) return reject(err)
        let _transport = data.Payload
        .pipe(new Transform({
          transform(chunk, encoding, callback) {
            callback(null, chunk.Records ? chunk.Records.Payload : undefined)
          },
          objectMode: true
        }))
        let ret = new PassThrough({objectMode: true})
        
        for (let { node } of recursive(data)) {
          if (node && typeof node.on === 'function') {
            node.on('error', err => ret.emit('error'))
          }
        }
        _transport.pipe(jsonstream.parse()).pipe(ret)
        resolve(ret)
      })
    })
  }

  const exports = {
    getObject,
    getStream,
    putObject,
    hasObject,
    storeBlock,
    upload,
    waitForStream,
    query
  }
  
  return exports
}

