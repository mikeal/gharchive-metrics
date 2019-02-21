const bent = require('./retry')
const mkdirp = require('mkdirp')
const fs = require('fs')
const path = require('path')
const createS3 = require('../src/shared/s3')
const gharchive = require('../src/shared/gharchive')
const inline = require('single-line-log').stdout
const { range } = require('./query')

const download = async (profile, key, outputDir) => {
  let s3 = createS3(profile)
  let down = s3.getStream(key)
  mkdirp.sync(outputDir)
  let filename = key.slice(key.lastIndexOf('/') + 0)
  down.pipe(fs.createWriteStream(path.join(outputDir, filename)))
  return new Promise((resolve, reject) => {
    let len = 0
    down.on('data', chunk => len += chunk.length)
    down.on('error', reject)
    down.on('end', () => resolve({ len, filename }))
  })
}

const pull = async (file, url, filter, profile, outputDir) => {
  let archive = await gharchive(file, url, retry = 0, log = console.log)
  if (!archive) return { fail: { archive } }
  let get = bent(url + '/filter', 'json')
  let result = await get(`?file=${file}&filter=${filter}`)
  return download(profile, result.cache, outputDir)
}
const mkQuery = async function * (profile, filter, timerange, url, parallelism, outputDir) {
  let s3 = createS3(profile)
  let reader = range(timerange, url, parallelism, () => {})
  let running = new Set()

  let iter = reader[Symbol.asyncIterator]()
  for (let i = 0; i < parallelism; i++) {
    let _run = () => {
      let p = iter.next()
      p.then(obj => {
        let { value, done } = obj
        let file = value
        running.delete(p)
        if (done) return

        let _p = pull(file, url, filter, profile, outputDir)
        _p.then(length => {
          running.delete(_p)
          _run()
          return length
        })
        running.add(_p)
      })
      running.add(p)
    }
    _run()
  }

  while (running.size) {
    let len = await Promise.race(Array.from(running))
    yield len
  }
}

const run = async (timerange, url, filter, profile, parallelism, outputDir) => {
  let q = mkQuery(profile, filter, timerange, url, parallelism, outputDir)
  for await (let {filename, len, fail} of q) {
    if (filename) inline(`write: ${filename} ${len} bytes`)
    else if (fail) console.error({fail})
  }
}

module.exports = run
