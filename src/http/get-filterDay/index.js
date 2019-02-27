const gharchive = require('@architect/shared/gharchive')
const bent = require('@architect/shared/retry')
const s3 = require('@architect/shared/s3')()

const pull = async (file, url, filter) => {
  if (!await s3.hasObject(`gharchive/${file}`)) {
    let archive = await gharchive(file, url, 3)
    if (!archive) return { fail: { archive } }
  }
  let get = bent(url, 'json')
  return get(`/filter?file=${file}&filter=${filter}`)
}

const run = async (day, url, filter) => {
  let i = 0
  let promises = []
  while (i < 24) {
    let file = day + '-' + i + '.json.gz'
    promises.push(pull(file, url, filter))
    i += 1
  }
  let results = await Promise.all(promises)
  return results.map(ret => ret.cache)
}

const geturl = req => {
  let proto = req.headers['X-Forwarded-Proto']
  let host = req.headers.Host
  let env = process.env.NODE_ENV
  let url = `${proto}://${host}/${env}`
  return url
}

exports.handler = async function http (req) {
  let { day, filter } = req.query
  let url = geturl(req)
  let ret = await run(day, url, filter)
  return {
    type: 'application/json; charset=utf8',
    body: JSON.stringify(ret)
  }
}
