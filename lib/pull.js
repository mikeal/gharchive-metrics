const fs = require('fs')
const createS3 = require('../src/shared/s3')
const inline = require('single-line-log').stderr
const createLambda = require('../src/shared/lambda')

const capitalize = str => str[0].toUpperCase() + str.slice(1)

const filterFiles = (type, argv) => {
  let lambda = createLambda(argv.profile)
  let query = { filter: argv.filter }
  query[type] = argv[type]
  query.limit = argv.limit
  let name = 'filter' + capitalize(type)
  return lambda(name, query)
}

const merge = async opts => {
  let lambda = createLambda(opts.profile)
  let ret
  try {
    ret = await lambda('concat', opts)
  } catch (e) {
    inline(`Waiting 2 seconds for S3 eventual consistency`)
    ret = await lambda('concat', opts)
  }
  return ret.cache
}

const _pull = (type) => async argv => {
  let s3 = createS3(argv.profile)
  inline(`${argv[type]}: Generating...`)
  let files = await filterFiles(type, argv)
  let len = files.length
  files = files.filter(f => f)
  if (files.length !== len) console.error(`BUG! ${len - files.length} file refs are null.`)
  inline(`${argv[type]}: Merging ${files.length} files.`)
  let cache = await merge({ files, limit: argv.limit || 950, profile: argv.profile })
  let download = s3.getStream(cache)
  let output = argv.outputFile ? fs.createWriteStream(argv.outputFile) : process.stdout
  return download.pipe(output)
}

exports.day = _pull('day')
exports.month = _pull('month')
exports.year = async argv => {
  let s3 = createS3(argv.profile)
  let months = []
  let i = 1
  while (months.length < 12) {
    let opts = Object.assign({}, argv)
    opts.month = argv.year + '-' + i.toString().padStart(2, '0')
    inline(`${opts.month}: Generating...`)
    let files = await filterFiles('month', opts)
    let len = files.length
    files = files.filter(f => f)
    if (files.length !== len) console.error(`BUG! ${len - files.length} file refs are null.`)
    inline(`${opts.month}: Merging ${files.length} files.`)
    let cache = await merge({ files, limit: argv.limit || 950, profile: argv.profile })
    months.push(cache)
    i++
  }
  if (argv.returnFiles) return months
  let cache = await merge({ files: months, profile: argv.profile })
  let download = s3.getStream(cache)
  let output = argv.outputFile ? fs.createWriteStream(argv.outputFile) : process.stdout
  return download.pipe(output)
}
exports.years = async argv => {
  let s3 = createS3(argv.profile)
  let opts = Object.assign({}, argv)
  let year = argv.start
  opts.returnFiles = true
  let files = []
  while (year <= argv.end) {
    opts.year = year
    let _files = await exports.year(opts)
    _files.forEach(f => files.push(f))
    year += 1
  }
  let cache = await merge({ files, profile: argv.profile })
  let download = s3.getStream(cache)
  let output = argv.outputFile ? fs.createWriteStream(argv.outputFile) : process.stdout
  return download.pipe(output)
}
