const createS3 = require('@architect/shared/s3')
const s3 = createS3()
const cbor = require('dag-cbor-sync')()
const jsonstream = require('jsonstream2')
const { createGzip, createGunzip } = require('zlib')

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

const pipeline = (source, repos, orgs, reject, uploader) => {
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
  let gz = createGzip()
  source.on('error', reject)
  gz.on('error', reject)
  _transform.on('error', reject)
  return source.pipe(_transform).pipe(gz).pipe(uploader)
}

const compose = (query, filter, file, repos, orgs) => new Promise((resolve, reject) => {
  query.on('error', reject)
  let uploader = s3.upload(`cache/${filter}/${file}`)
  uploader.on('error', reject)
  pipeline(query, repos, orgs, reject, uploader)
  uploader.on('finish', () => resolve({
    type: 'application/json',
    body: JSON.stringify({
      cache: `cache/${filter}/${file}`,
      orgs,
      repos,
      filter,
      file
    })
  }))
})

let _file
exports.handler = async function http (req) {
  let { filter, file } = req.query
  console.error({ file })
  _file = file
  let { orgs, repos, keys } = cbor.deserialize(await s3.getObject(`blocks/${filter}`))
  keys.push('repo.name')
  keys.push('created_at')
  keys = Array.from(new Set(keys))
  let source = fallback(file, keys)
  return compose(source, filter, file, repos, orgs)
}

process.on('uncaughtException', (err) => {
  console.error({ file: _file, message: err.message })
  console.error(err)
  process.exit(1)
})
