const { counter, quarter } = require('reed-richards')
const { PassThrough } = require('stream')
const jsonstream = require('jsonstream2')
const mkcsv = require('mkcsv')
const mkdirp = require('mkdirp')
const fs = require('fs').promises
const { createReadStream } = require('fs')
const path = require('path')

const collections = {
  quarterlyContributions: counter(
    'quarter', 'type'
  ),
  quarterlyContributors: counter(
    'quarter', 'login'
  ),
  quarterlyContributorsByType: counter(
    'quarter', 'type', 'login'
  )
}

const finish = outputDir => {
  for (let [name, collection] of Object.entries(collections)) {
    let output = mkcsv(Array.from(collection.objects()))
    fs.writeFile(path.join(outputDir, `${name}.csv`), output)
  }
}

const logline = obj => {
  obj.quarter = quarter(new Date(obj.created_at))
  for (let collection of Object.values(collections)) {
    if (collection.count) collection.count(obj)
    else if (collection.add) collection.add(obj)
  }
}

const processStream = async (parser, logline = logline) => {
  let stream = parser.pipe(new PassThrough({ objectMode: true }))
  for await (let obj of stream) {
    await logline(obj)
  }
}

const run = async (dataFile, outputDir, logline = logline, finish = finish) => {
  mkdirp.sync(outputDir)
  let parser = jsonstream.parse()
  let finished = processStream(parser, logline)
  let read
  createReadStream(dataFile).pipe(parser)
  await finished
  finish(outputDir)
}

// run('2018-pl-data', 'test-metrics')
module.exports = run
