const { counter, collection, quarter } = require('reed-richards')
const { PassThrough } = require('stream')
const jsonstream = require('jsonstream2')
const mkcsv = require('mkcsv')
const mkdirp = require('mkdirp')
const fs = require('fs').promises
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

const processStream = async parser => {
  let stream = parser.pipe(new PassThrough({ objectMode: true }))
  for await (let obj of stream) {
    await logline(obj)
  }
}

const run = async (dataDir, outputDir) => {
  mkdirp.sync(outputDir)
  let filenames = await fs.readdir(dataDir)
  let parser = jsonstream.parse()
  let finished = processStream(parser)
  let read
  while (filenames.length) {
    let filename = filenames.shift()
    let _data = fs.readFile(path.join(dataDir, filename))
    _data.then(data => {
      parser.write(data)
      parser.write(Buffer.from('\n'))
    })
    if (read) await read
    read = _data
  }
  await read
  parser.end()
  await finished
  finish(outputDir)
}

// run('2018-pl-data', 'test-metrics')
module.exports = run

