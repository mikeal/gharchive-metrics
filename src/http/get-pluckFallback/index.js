const createS3 = require('@architect/shared/s3')
const s3 = createS3()
const jsonstream = require('jsonstream2')
const { createGunzip } = require('zlib')
const crypto = require('crypto')
const hash = str => crypto.createHash('sha256').update(str).digest('hex')

const { Transform } = require('stream')

const streamWait = stream => new Promise((resolve, reject) => {
  stream.on('finish', resolve)
  stream.on('error', reject)
})

const fallback = (file, keys) => {
  let _transform = new Transform({
    writableObjectMode: true,
    objectMode: true,
    transform (obj, encoding, callback) {
      let _obj = {}
      keys.forEach(key => {
        let __obj = obj
        let _key = key.split('.')
        let k
        while (_key.length > 0) {
          k = _key.shift()
          __obj = __obj[k]
        }
        _obj[k] = __obj
      })
      if (Object.keys(_obj).length) {
        callback(null, Buffer.from(JSON.stringify(_obj) + '\n'))
      } else {
        callback(null)
      }
    }
  })
  return s3.getStream(`gharchive/${file}`)
    .pipe(createGunzip())
    .pipe(jsonstream.parse())
    .pipe(_transform)
}

exports.handler = async function http (req) {
  let { file, keys } = req.query
  keys = keys.split(',')
  keys.push('repo.name')
  keys.push('created_at')
  keys = keys.filter(k => k)

  let cachekey = `cache/pluck/${hash(file + keys.sort().join(','))}.json`
  let stream = fallback(file, keys)
  let upload = s3.upload(cachekey)
  upload.write('\n')
  stream.pipe(upload)

  await streamWait(upload)
  return {
    body: JSON.stringify({ cache: cachekey }),
    type: 'application/json; charset=utf8'
  }
}
