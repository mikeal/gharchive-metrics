const s3 = require('@architect/shared/s3')()
const { Transform } = require('stream')

const stringify = () => new Transform({
  transform (obj, encoding, callback) {
    callback(null, Buffer.from(JSON.stringify(obj) + '\n'))
  },
  objectMode: true
})

const streamWait = stream => new Promise((resolve, reject) => {
  stream.on('finish', resolve)
  stream.on('error', reject)
})

exports.handler = async function http (req) {
  let { file, keys } = req.query
  keys = keys.split(',')
  let cachekey = `cache/pluck/${file}-${encodeURIComponent(keys.sort().join(','))}.json`
  keys.push('repo.name')
  keys.push('created_at')
  keys = Array.from(new Set(keys))
  keys = keys.map(k => 's.' + k).join(', ')
  let sql = `SELECT ${keys} from S3Object s`
  let query = await s3.query(sql, `gharchive/${file}`)

  let stream = query.pipe(stringify())
  let upload = s3.upload(cachekey)
  stream.pipe(upload)
  await streamWait(upload)
  return {
    body: JSON.stringify({ cache: cachekey }),
    type: 'application/json; charset=utf8'
  }
}
