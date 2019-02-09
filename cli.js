const ghTimerange = require('./lib/gh-timerange')
const pkg = require('./package.json')       
const log = require('single-line-log').stdout
const createS3 = require('./lib/s3')

const range = (argv, log) => {
  let [start, end] = argv.timerange.split(':')
  let reader = ghTimerange(start, end, argv.url, argv.parallelism, log)
  return reader
}

const prime = async argv => {
  let reader = range(argv, log)
  for await (let file of reader) {
    log(`cached ${file}`)
  }
}

const query = async function * (s3, reader, sql) {
  for await (let file of reader) {
    log(file)
    let q = await s3.query(sql, `gharchive/${file}`)
    yield * q
  }
}

const runQuery = async argv => {
  let s3 = createS3(argv.profile)
  let reader = range(argv, () => {})
  for await (let obj of query(s3, reader, argv.sql)) {
    // console.log(obj)
  }
}

const timerangeOptions = yargs => {
  yargs.option('parallelism', {
    desc: 'Max number of concurrent requests',
    default: 100
  })
  yargs.option('url', {
    desc: 'Cache service URL',
    default: pkg.productionURL
  })
}

require('yargs')
  .command({
    command: 'prime [timerange]',
    builder: yargs => {
      yargs.positional('timerange', {
        desc: 'Timerange in format: 2019-01-01:2019-20-01',
        required: true
      })
      timerangeOptions(yargs)
    },
    desc: 'Primes the gharchive tarball cache of the timerange.',
    handler: prime
  })
  .command({
    command: 'query <timerange> <sql> <profile>',
    builder: yargs => {
      yargs.positional('timerange', {
        desc: 'Timerange in format: 2019-01-01:2019-01-02',
        required: true
      })
      yargs.positional('sql', {
        desc: 'SQL Query for S3 select',
        required: true
      })
      yargs.positional('profile', {
        required: true,
        desc: 'AWS profile name'
      })
      timerangeOptions(yargs)
    },
    desc: 'Queries a range of gharchive tarballs',
    handler: runQuery
  })
  .argv

