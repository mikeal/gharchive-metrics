const createS3 = require('@architect/shared/s3')
const s3 = createS3()
const cbor = require('dag-cbor-sync')(655360)
const jsonstream = require('jsonstream2')
const { Transform } = require('stream')
const lambda = require('@architect/shared/lambda')()

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
    console.error('FALLBACK')
    resp = await lambda('pluckFallback', { file, keys: keys.join(',') })
  }
  return s3.getStream(resp.cache)
}

const createFilter = (repos, orgs) => {
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
  return _f
}

const streamWait = stream => new Promise((resolve, reject) => {
  stream.on('finish', resolve)
  stream.on('error', reject)
})

exports.handler = async function http (req) {
  let { filter, file } = req.query
  console.error({ file })
  let { orgs, repos, keys } = cbor.deserialize(await s3.getObject(`blocks/${filter}`))

  let cachekey = `cache/pluck/${file}-${encodeURIComponent(keys.sort().join(','))}.json`
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
  stream.pipe(upload)
  await streamWait(upload)
  return {
    type: 'application/json',
    body: JSON.stringify({
      cache,
      orgs,
      repos,
      filter,
      file
    })
  }
}
