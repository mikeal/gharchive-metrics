const gharchive = require('../src/shared/gharchive')

const onehour = 1000 * 60 * 60
const oneday = onehour * 24
const timerange = function * (start, end) {
  if (!(start instanceof Date)) start = new Date(start)
  if (!(end instanceof Date)) end = new Date(end)
  let ts = start
  while (ts < end) {
    let year = ts.getUTCFullYear()
    let month = (ts.getUTCMonth() + 1).toString().padStart(2, '0')
    let day = ts.getUTCDate().toString().toString().padStart(2, '0')
    let hour = ts.getUTCHours()
    yield `${year}-${month}-${day}-${hour}.json.gz`
    ts = new Date(ts.getTime() + onehour)
  }
}

const ghtimerange = async function * (start, end, url, parallelism = 1, log = console.log) {
  let iter = timerange(start, end)
  let running = new Set()
  let _run = filename => {
    let promise = gharchive(filename, url, 0, log).then(ret => {
      running.delete(promise)
      if (ret) return filename
      else return ret
    })
    running.add(promise)
  }
  for (let i = 0; i < parallelism; i++) {
    let { value, done } = iter.next()
    if (value) _run(value)
  }
  while (running.size) {
    let filename = await Promise.race(Array.from(running))
    if (filename) yield filename
    let { value, done } = iter.next()
    if (value) _run(value)
  }
}

module.exports = ghtimerange

// ;(async () => {
//  for await (let x of ghtimerange('2018-01-01', '2018-01-02')) {
//    console.log(x)
//  }
// })()
