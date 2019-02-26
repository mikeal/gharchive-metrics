#!/bin/sh
':' // # comment; exec /usr/bin/env node --experimental-worker "$0" "$@"
const bent = require('bent')
const pkg = require('./package.json')
const log = require('single-line-log').stdout
const mkfilter = require('./lib/mkfilter')
const filter = require('./lib/filter')
const mkQuery = require('./lib/query')
const pull = require('./lib/pull')
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
  filter(argv.profile, argv.timerange, argv.url, argv.parallelism, argv.filter)
}

const urlOption = yargs => {
  yargs.option('url', {
    desc: 'Cache service URL',
    default: pkg.productionURL
  })
}
const parallelismOption = yargs => {
  yargs.option('parallelism', {
    desc: 'Max number of concurrent requests',
    default: 20
  })
}
const timerangeOptions = yargs => {
  parallelismOption(yargs)
  urlOption(yargs)
}
const profileOption = yargs => {
  yargs.positional('profile', {
    required: true,
    desc: 'AWS profile name'
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
  profileOption(yargs)
  timerangeOptions(yargs)
}
const filterOption = yargs => {
  yargs.positional('filter', {
    desc: 'Hash of of the filter options, created with mkfilter',
    required: true
  })
}
const pullOptions = yargs => {
  filterOption(yargs)
  yargs.positional('outputDir', {
    desc: 'Directory to write filtered activity.',
    required: true
  })
}

const downloadOptions = yargs => {
  profileOption(yargs)
  urlOption(yargs)
  pullOptions(yargs)
}

const dayOption = yargs => {
  yargs.positional('day', {
    desc: 'Day to query in format "2018-01-01"'
  })
}
const monthOption = yargs => {
  yargs.positional('month', {
    desc: 'Month to query in format "2018-01"'
  })
}
const yearOption = yargs => {
  yargs.positional('year', {
    desc: 'Year to query in format "2018"'
  })
}

require('yargs') // eslint-disable-line
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
    command: 'filter <timerange> <profile> <filter>',
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
      yargs.option('keys', {
        desc: 'Command delimited list of keys to include in filtered object'
      })
      yargs.option('orgs', {
        desc: 'Comma delimited list of orgs to filter on'
      })
      yargs.option('repos', {
        desc: 'Comma delimited list of repos to filter on'
      })
      profileOption(yargs)
    },
    desc: 'Creates and stores CBOR node for filter parameters',
    handler: async argv => {
      let d = key => argv[key] ? argv[key].split(',') : []
      let block = await mkfilter(d('keys'), d('orgs'), d('repos'), argv.profile)
      console.log(block.cid.toBaseEncodedString())
    }
  })
  .command({
    command: 'pull <timerange> <profile> <filter> <outputDir>',
    desc: 'Pull resources through remote filter',
    handler: async argv => {
      pull(argv.timerange, argv.url, argv.filter, argv.profile, argv.parallelism, argv.outputDir)
    },
    builder: yargs => {
      parallelismOption(yargs)
      queryOptions(yargs)
      pullOptions(yargs)
    }
  })
  .command({
    command: 'filter-day <day> <filter>',
    desc: 'Pull resources through remote filter for a specific day',
    handler: async argv => {
      let get = bent(argv.url, 'json')
      let ret = await get(`/filterDay?day=${argv.day}&filter=${argv.filter}`)
      console.log(ret)
    },
    builder: yargs => {
      urlOption(yargs)
      filterOption(yargs)
      dayOption(yargs)
    }
  })
  .command({
    command: 'filter-month <month> <filter>',
    desc: 'Pull resources through remote filter for a specific month',
    handler: async argv => {
      let get = bent(argv.url, 'json')
      let ret = await get(`/filterMonth?month=${argv.month}&filter=${argv.filter}`)
      console.log(ret)
    },
    builder: yargs => {
      urlOption(yargs)
      monthOption(yargs)
      filterOption(yargs)
    }
  })
  .command({
    command: 'pull-day <day> <filter> <profile> <outputDir>',
    desc: 'Download resources through remote filter for a specific day',
    handler: pull.day,
    builder: yargs => {
      dayOption(yargs)
      downloadOptions(yargs)
    }
  })
  .command({
    command: 'pull-month <month> <filter> <profile> <outputDir>',
    desc: 'Download resources through remote filter for a specific month',
    handler: pull.month,
    builder: yargs => {
      monthOption(yargs)
      downloadOptions(yargs)
    }
  })
  .command({
    command: 'pull-year <year> <filter> <profile> <outputDir>',
    desc: 'Download resources through remote filter for a specific year',
    handler: pull.year,
    builder: yargs => {
      yearOption(yargs)
      downloadOptions(yargs)
    }
  })
  .argv
