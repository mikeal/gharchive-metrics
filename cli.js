const ghTimerange = require('./lib/gh-timerange')
const pkg = require('./package.json')       
const log = require('single-line-log').stdout
const createS3 = require('./src/shared/s3')
const mkfilter = require('./lib/mkfilter')
const filter = require('./lib/filter')
const mkQuery = require('./lib/query')
const range = mkQuery.range

const prime = async argv => {
  let reader = range(argv.timerange, argv.url, argv.parallelism, log)
  for await (let file of reader) {
    log(`cached ${file}`)
  }
}

const runQuery = async argv => {
  let iter = mkQuery(argv.profile, argv.sql, argv.timerange, argv.url, argv.parallelism)
  for await (let line of iter) {
    console.log(line)
  }
}

const runFilter = async argv => {
  filter(argv.profile, argv.sql, argv.timerange, argv.url, argv.parallelism, argv.filter)
}

const timerangeOptions = yargs => {
  yargs.option('parallelism', {
    desc: 'Max number of concurrent requests',
    default: 20
  })
  yargs.option('url', {
    desc: 'Cache service URL',
    default: pkg.productionURL
  })
}

const queryOptions = yargs => {
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
      queryOptions(yargs)
    },
    desc: 'Queries a range of gharchive tarballs',
    handler: runQuery
  })
  .command({
    command: 'filter <timerange> <sql> <profile> <filter>',
    builder: yargs => {
      queryOptions(yargs)
      yargs.positional('filter', {
        desc: 'Hash of of the filter options, created with mkfilter'
      })
    },
    desc: 'Filters the given query by set repo filter params',
    handler: argv => runFilter(argv)
  })
  .command({
    command: 'mkfilter <profile>',
    builder: yargs => {
      yargs.option('orgs', {
        desc: 'Comma delimited list of orgs to filter on'
      })
      yargs.option('repos', {
        desc: 'Comma delimited list of repos to filter on'
      })
      yargs.positional('profile', {
        required: true,
        desc: 'AWS profile name'
      })
    },
    desc: 'Creates and stores CBOR node for filter parameters',
    handler: async argv => {
      let d = key => argv[key] ? argv[key].split(',') : []
      let block = await mkfilter(d('orgs'), d('repos'), argv.profile)
      console.log(block.cid.toBaseEncodedString())
    }
  })
  .argv

