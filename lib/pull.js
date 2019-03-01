const fs = require('fs')
const path = require('path')
const createS3 = require('../src/shared/s3')
const gharchive = require('../src/shared/gharchive')
const inline = require('single-line-log').stdout
const download = require('../src/shared/download')
const createLambda = require('../src/shared/lambda')

const capitalize = str => str[0].toUpperCase() + str.slice(1)

const _pull = (type) => async argv => {
  console.error('Generating...')
  let lambda = createLambda(argv.profile)
  let s3 = createS3(argv.profile)
  let query = { filter: argv.filter }
  query[type] = argv[type]
  let name = 'filter' + capitalize(type)
  let files = await lambda(name, query)
  console.error(`Service generated ${files.length} filtered files. Downloading...`)
  let { cache } = await lambda('concat', {files})
  let download = s3.getStream(cache)
  let output = argv.outputFile ? fs.createWriteStream(argv.outputFile) : process.stdout
  return download.pipe(output)
}

exports.day = _pull('day')
exports.month =  _pull('month')
exports.year = async argv => {
  if (!argv.log) argv.log = console.log
  let log = argv.log
  mkdirp.sync(argv.outputDir)
  let months = []
  while (months.length < 12) {
    months.push(`${argv.year}-${(months.length + 1).toString().padStart(2, '0')}`)
  }
  let pending = []
  let _downloading = false
  let last

  let lambda = createLambda(argv.profile)

  while (months.length) {
    let month = months.shift()
    let ret = await lambda('filterMonth', { month, filter: argv.filter })
    log(`Service generated ${ret.length} filtered files for ${month}.`)
    let _download = () => {
      _downloading = true
      last = download.all(argv.profile, ret, argv.outputDir, 100)
      last.then(downloaded => {
        _downloading = false
        let p
        if (pending.length) {
          p = pending.shift()()
        }
        downloaded.services.forEach(service => service.terminate())
        log(`Downloaded ${downloaded.length} files for ${month}.`)
        return p
      })
      return last
    }
    pending.push(_download)
    if (!_downloading) pending.shift()()
  }
  await last
}
