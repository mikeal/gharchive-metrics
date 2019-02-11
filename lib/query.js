const ghTimerange = require('./gh-timerange')
const log = require('single-line-log').stdout
const createS3 = require('../src/shared/s3')

const range = (timerange, url, parallelism, log) => {
  let [start, end] = timerange.split(':')
  let reader = ghTimerange(start, end, url, parallelism, log)
  return reader
}

const query = async function * (s3, reader, sql) {
  for await (let file of reader) {
    log(file)
    yield s3.query(sql, `gharchive/${file}`)
  }
}

const mkQuery = async function * (profile, sql, timerange, url, parallelism) {
  let s3 = createS3(profile)
  let reader = range(timerange, url, parallelism, () => {})
  let running = new Set()

  let iter = reader[Symbol.asyncIterator]()
  for (let i = 0; i < parallelism; i++) {
    let _run = () => {
      let p = iter.next()
      p.then(obj => {
        let {value, done} = obj
        let file = value
        running.delete(p)
        let qp = s3.query(sql, `gharchive/${file}`)
        qp.then(lines => {
          lines = lines[Symbol.asyncIterator]()
          running.delete(qp)
          let __run = () => {
            let p = lines.next()
            p.then(obj => {
             let {value, done} = obj
             running.delete(p)
              if (done) {
                _run()
              } else { 
                __run()
              }
            })
            running.add(p)
          }
          __run()
        })
        running.add(qp)
      })
      running.add(p)
    }
    _run()
  }

  while (running.size) {
    let {value, done} = await Promise.race(Array.from(running))
    if (value && typeof value === 'object') {
      yield value
    }
  }
}

module.exports = mkQuery
module.exports.query = query
