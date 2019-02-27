const createS3 = require('@architect/shared/s3')
const s3 = createS3()
const jsonstream = require('jsonstream2')
const { createGunzip } = require('zlib')

const { Transform } = require('stream')

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
        callback(null, _obj)
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

const collect = stream => new Promise((resolve, reject) => {
  let objs = []
  stream.on('data', obj => objs.push(obj))
  stream.on('end', resolve(objs))
  stream.on('error', reject)
})

exports.handler = async function http (req) {
  let { file, keys } = req.query
  keys = keys.split(',')
  keys.push('repo.name')
  keys.push('created_at')

  let cachekey = `cache/pluck/${file}-${encodeURIComponent(keys.sort().join(','))}.json`
  let query = fallback(file, keys)
  let objs = await collect(query)
  await s3.putObject(cachekey, Buffer.from(JSON.stringify(objs)))
  return {
    body: JSON.stringify({ cache: cachekey }),
    type: 'application/json; charset=utf8'
  }
}
