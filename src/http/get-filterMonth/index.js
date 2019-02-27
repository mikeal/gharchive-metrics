const bent = require('@architect/shared/retry')

const oneday = 1000 * 60 * 60 * 24

const pull = async (day, url, filter) => {
  let get = bent(url + '/filterDay', 'json')
  return get(`?day=${day}&filter=${filter}`)
}

const flatten = arrays => [].concat.apply([], arrays)

const run = async (month, url, filter) => {
  let promises = []
  let ts = new Date(month + '-01')
  month = ts.getMonth()
  while (ts.getMonth() === month) {
    let day = ts.toISOString().slice(0, 10)
    console.error({ day })
    let p = pull(day, url, filter)
    promises.push(p)
    p.then(() => console.log({ finish: day }))
    ts = new Date(ts.getTime() + oneday)
  }
  let results = await Promise.all(promises)
  return flatten(results)
}

const geturl = req => {
  let proto = req.headers['X-Forwarded-Proto']
  let host = req.headers.Host
  let env = process.env.NODE_ENV
  let url = `${proto}://${host}/${env}`
  return url
}

exports.handler = async function http (req) {
  let { month, filter } = req.query
  let url = geturl(req)
  let ret = await run(month, url, filter)
  return {
    type: 'application/json; charset=utf8',
    body: JSON.stringify(ret)
  }
}
