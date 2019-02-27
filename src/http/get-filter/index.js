const createS3 = require('@architect/shared/s3')
const s3 = createS3()
const cbor = require('dag-cbor-sync')(655360)
const bent = require('bent')

const pluck = async (file, url, keys) => {
  let get = bent(url, 'json')
  console.log({ file, get: 'pluck' })
  let resp
  try {
    resp = await get(`/pluck?file=${file}&keys=${keys.join(',')}`)
  } catch (e) {
    console.error('FALLBACK')
    resp = await get(`/pluckFallback?file=${file}&keys=${keys.join(',')}`)
  }
  return s3.getObject(resp.cache)
}

const geturl = req => {
  let proto = req.headers['X-Forwarded-Proto']
  let host = req.headers.Host
  let env = process.env.NODE_ENV
  let url = `${proto}://${host}/${env}`
  return url
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

exports.handler = async function http (req) {
  let { filter, file } = req.query
  console.error({ file })
  let url = geturl(req)
  let { orgs, repos, keys } = cbor.deserialize(await s3.getObject(`blocks/${filter}`))

  let cachekey = `cache/pluck/${file}-${encodeURIComponent(keys.sort().join(','))}.json`
  let buffer
  if (!await s3.hasObject(cachekey)) {
    buffer = await pluck(file, url, keys)
  } else {
    buffer = await s3.getObject(cachekey)
  }
  let objs = JSON.parse(buffer.toString())
  let _filter = createFilter(repos, orgs)
  let data = Buffer.from(JSON.stringify(objs.filter(_filter)))
  let cache = `cache/${filter}/${file.slice(0, file.lastIndexOf('.gz'))}`
  await s3.putObject(cache, data)
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
