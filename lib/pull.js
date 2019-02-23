const bent = require('../src/shared/retry')
const mkdirp = require('mkdirp')
const fs = require('fs')
const path = require('path')
const createS3 = require('../src/shared/s3')
const gharchive = require('../src/shared/gharchive')
const inline = require('single-line-log').stdout
const { range } = require('./query')
const download = require('../src/shared/download')

const pull = async (file, url, filter, profile, outputDir) => {
  let archive = await gharchive(file, url, retry = 0, log = console.log)
  if (!archive) return { fail: { archive } }
  let get = bent(url + '/filter', 'json')
  let result = await get(`?file=${file}&filter=${filter}`)
  return download(profile, result.cache, outputDir)
}
const mkQuery = async function * (profile, filter, timerange, url, parallelism, outputDir) {
  let s3 = createS3(profile)
  mkdirp.sync(outputDir)
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
module.exports.day = async argv => {
  mkdirp.sync(argv.outputDir)
  console.log('Generating...')
  let get = bent(argv.url, 'json')
  let ret = await get(`/filterDay?day=${argv.day}&filter=${argv.filter}`)
  console.log(`Service generated ${ret.length} filtered files. Downloading...`)
  let downloaded = await download.all(argv.profile, ret, argv.outputDir)
  console.log(`Downloaded ${downloaded.length} files.`)
  process.exit()
}
module.exports.month = async argv => {
  mkdirp.sync(argv.outputDir)
  console.log('Generating...')
  let get = bent(argv.url, 'json')
  let ret = await get(`/filterMonth?month=${argv.month}&filter=${argv.filter}`)
  console.log(`Service generated ${ret.length} filtered files. Downloading...`)
  let downloaded = await download.all(argv.profile, ret, argv.outputDir)
  console.log(`Downloaded ${downloaded.length} files.`)
  process.exit()
}
module.exports.year = async argv => {
  mkdirp.sync(argv.outputDir)
  let months = []
  while (months.length < 12) {
    months.push(`${argv.year}-${(months.length+1).toString().padStart(2, '0')}`)
  }
  let get = bent(argv.url, 'json')
  let pending = []
  let _downloading = false
  let last
  while (months.length) {
    let month = months.shift()
    let ret = await get(`/filterMonth?month=${month}&filter=${argv.filter}`)
    console.log(`Service generated ${ret.length} filtered files for ${month}.`)
    let _download = () => {
      _downloading = true
      last = download.all(argv.profile, ret, argv.outputDir, 100)
      last.then(downloaded => {
        _downloading = false
        if (pending.length) pending.shift()()
        downloaded.services.forEach(service => service.terminate())
        console.log(`Downloaded ${downloaded.length} files for ${month}.`)
      })
    }
    pending.push(_download)
    if (!_downloading) pending.shift()()
  }
  await last
}

