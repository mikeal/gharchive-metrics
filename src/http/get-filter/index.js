const createS3 = require('@architect/shared/s3')
const s3 = createS3()
const jsonstream = require('jsonstream2')
const { Transform } = require('stream')
const lambda = require('@architect/shared/lambda')()
const crypto = require('crypto')

const hash = str => crypto.createHash('sha256').update(str).digest('hex')

const filterTransform = _f => new Transform({
  transform (obj, encoding, callback) {
    if (_f(obj)) callback(null, Buffer.from(JSON.stringify(obj) + '\n'))
    else callback(null)
  },
  objectMode: true
})

const pluck = async (file, keys) => {
  let resp
  try {
    resp = await lambda('pluck', { file, keys: keys.join(',') })
  } catch (e) {
    resp = await lambda('pluckFallback', { file, keys: keys.join(',') })
  }
  return s3.getStream(resp.cache)
}

const createFilter = (repos, orgs) => {
  repos = new Set(repos)
  let _f = r => {
    if (r.name) {
      // has a repo name
      if (repos.has(r.name)) return true

      for (let org of orgs) {
        if (r.name.startsWith(`${org}/`)) return true
      }
    }
  }
  return _f
}

const streamWait = stream => new Promise((resolve, reject) => {
  stream.on('finish', resolve)
  stream.on('error', reject)
})

exports.handler = async function http (req) {
  let { filter, file, cborSize } = req.query
  cborSize = cborSize || 655360
  let cbor = require('dag-cbor-sync')(cborSize)
  console.log({ filter, file, cborSize })
  let { orgs, repos, keys } = cbor.deserialize(await s3.getObject(`blocks/${filter}`))
  if (!keys) keys = []
  keys = keys.filter(k => k)

  let cachekey = `cache/pluck/${hash(file + keys.sort().join(','))}.json`
  let plucked
  if (!await s3.hasObject(cachekey)) {
    plucked = await pluck(file, keys)
  } else {
    plucked = await s3.getStream(cachekey)
  }
  let cache = `cache/${filter}/${file.slice(0, file.lastIndexOf('.gz'))}`
  let upload = s3.upload(cache)
  let _f = createFilter(repos, orgs)
  let stream = plucked.pipe(jsonstream.parse()).pipe(filterTransform(_f))
  upload.write(Buffer.from('\n'))
  stream.pipe(upload)
  await streamWait(upload)
  return {
    type: 'application/json',
    body: JSON.stringify({
      cache,
      orgs,
      filter,
      file
    })
  }
}
