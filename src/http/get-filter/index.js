// @architect/functions enables secure sessions, express-style middleware and more
// let arc = require('@architect/functions')
// let url = arc.http.helpers.url

const createS3 = require('@architect/shared/s3')
const s3 = createS3()
const cbor = require('dag-cbor-sync')()
const jsonstream = require('jsonstream2')
const { createGzip, createGunzip } = require('zlib')

const { Transform } = require('stream')

const geturl = req => {
  let proto = req.headers['X-Forwarded-Proto']
  let host = req.headers.Host
  let env = process.env.NODE_ENV
  let url = `${proto}://${host}/${env}`
  return url
}

const fallback = (file, keys, uploader) => {
  let _transform = new Transform({
    writableObjectMode: true,
    objectMode: true,
    transform (obj, encoding, callback) {
      let _obj = {}
      keys.forEach(key => {
        let _key = key.split('.')
        let k
        while (_key.length > 0) {
          k = _key.shift()
          obj = obj[k]
        }
        _obj[k] = obj
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

exports.handler = async function http (req) {
  // let url = geturl(req)
  let { filter, file } = req.query
  let { orgs, repos, keys } = cbor.deserialize(await s3.getObject(`blocks/${filter}`))
  keys.push('repo.name')
  keys.push('created_at')
  keys = Array.from(new Set(keys))
  keys = keys.map(k => 's.' + k).join(', ')
  let sql = `SELECT ${keys} from S3Object s`
  let query = await s3.query(sql, `gharchive/${file}`)
  let uploader = s3.upload(`cache/${filter}/${file}`)
  const pipeline = source => {
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
      transform (obj, encoding, callback) {
        if (_f(obj)) {
          callback(null, JSON.stringify(obj) + '\n')
        } else {
          callback(null)
        }
      }
    })
    source.pipe(_transform).pipe(createGzip()).pipe(uploader)
  }
  console.error({ filter, file })
  query.on('error', err => {
    console.error('failing')
    uploader.destroy(err)
    console.error('after failed')
    let source = fallback(file, keys, s3.upload(`cache/${filter}/${file}`))
    pipeline(source)
  })
  return new Promise((resolve, reject) => {
    uploader.on('finish', () => resolve({
      type: 'application/json',
      body: JSON.stringify({
        cache: `cache/${filter}/${file}`,
        sql,
        orgs,
        repos,
        keys,
        filter,
        file
      })
    }))
    uploader.on('error', reject)
  })
}
