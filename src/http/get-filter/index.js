// @architect/functions enables secure sessions, express-style middleware and more
// let arc = require('@architect/functions')
// let url = arc.http.helpers.url

const createS3 = require('@architect/shared/s3')
const s3 = createS3()
const cbor = require('dag-cbor-sync')()
const { createGzip } = require('zlib')

const { Transform } = require('stream')

const geturl = req => {
  let proto = req.headers['X-Forwarded-Proto']
  let host = req.headers.Host
  let env = process.env.NODE_ENV
  let url = `${proto}://${host}/${env}`
  return url
}

exports.handler = async function http(req) {
  console.log(req)
  let url = geturl(req)
  let {filter, file} = req.query
  let {orgs, repos, keys} = cbor.deserialize(await s3.getObject(`blocks/${filter}`)) 
  keys.push('repo.name') 
  keys.push('created_at')
  keys = Array.from(new Set(keys))
  keys = keys.map(k => 's.' + k).join(', ')
  let sql = `SELECT ${keys} from S3Object s`
  let query = await s3.query(sql, `gharchive/${file}`)
  let uploader = s3.upload(`cache/${filter}/${file}`)
  let _f = r => {
    if (r.name) {
      // has a repo name
      for (let repo of repos) {
        if (r.name === repo) return true
      }
      for (let org of orgs) {
        if (r.name.startsWith(`${org}/`)) return true
      }
    }
  } 
  const _transform = new Transform({
    writableObjectMode: true,
    objectMode: true,
    transform(obj, encoding, callback) {
      if (_f(obj)) {
        callback(null, JSON.stringify(obj) + '\n')
      } else {
        callback(null)
      }
    }
  })
  query.pipe(_transform).pipe(createGzip()).pipe(uploader)
  return new Promise((resolve, reject) => {
    uploader.on('finish', () => resolve({
      type: 'application/json',
      body: JSON.stringify({
        cache: `cache/${filter}/${file}`,
        sql, orgs, repos, keys, filter, file
      })
    }))
    uploader.on('error', reject)
  })
}

