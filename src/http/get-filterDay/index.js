const gharchive = require('@architect/shared/gharchive')
const bent = require('@architect/shared/retry')

const pull = async (file, url, filter) => {
  let archive = await gharchive(file, url, 3)
  if (!archive) return { fail: { archive } }
  let get = bent(url, 'json')
  let ret
  try {
    ret = await get(`/filter?file=${file}&filter=${filter}`)
  } catch (e) {
    try {
      ret = await get(`/filterManually?file=${file}&filter=${filter}`)
    } catch (e) {
      console.error({ fail: file })
      throw e
    }
  }
  return ret
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

let _day
exports.handler = async function http (req) {
  let { day, filter } = req.query
  _day = day
  let url = geturl(req)
  try {
    let ret = await run(day, url, filter)
    return {
      type: 'application/json; charset=utf8',
      body: JSON.stringify(ret)
    }
  } catch (e) {
    console.error('S3 Select errored, falling back.')
    throw e
  }
}

process.on('uncaughtException', err => {
  console.error({ day: _day, message: err.message })
  console.error(err)
  process.exit(1)
})
