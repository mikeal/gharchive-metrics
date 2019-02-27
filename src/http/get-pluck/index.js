const s3 = require('@architect/shared/s3')()

const collect = stream => new Promise((resolve, reject) => {
  let objs = []
  stream.on('data', obj => objs.push(obj))
  stream.on('end', () => resolve(objs))
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

  let objs = await collect(query)
  await s3.putObject(cachekey, Buffer.from(JSON.stringify(objs)))
  return {
    body: JSON.stringify({ cache: cachekey }),
    type: 'application/json; charset=utf8'
  }
}
