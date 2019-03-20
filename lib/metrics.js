const { counter, quarter } = require('reed-richards')
const { PassThrough } = require('stream')
const jsonstream = require('jsonstream2')
const mkcsv = require('mkcsv')
const mkdirp = require('mkdirp')
const fs = require('fs').promises
const { createReadStream } = require('fs')
const path = require('path')

const collections = {
  /*  quarterlyContributions: counter(
    'quarter', 'type'
  ),
  quarterlyContributors: counter(
    'quarter', 'login'
  ),
  quarterlyContributorsByType: counter(
    'quarter', 'type', 'login'
  ),*/
  quarterlyActivePassive: counter(
    'quarter', 'grouping', 'login'
  )
}

const finish = outputDir => {
  let coll = collections.quarterlyActivePassive
  for (let [quarter, value] of coll.data.entries()) {
    for (let [group, _map] of value.entries()) {
      if (group === 'inactive') {
        let active = coll.data.get(quarter).get('active')
        for (let key of _map.keys()) {
          active.delete(key)
        }
      }
    }
  }
  for (let [name, collection] of Object.entries(collections)) {
    let output = mkcsv(Array.from(collection.objects()))
    fs.writeFile(path.join(outputDir, `${name}.csv`), output)
    output = mkcsv(Array.from(collection.unique()))
    fs.writeFile(path.join(outputDir, `${name}-unique.csv`), output)
  }
}

const activeTypes = new Set([
  'PushEvent', 'CreateEvent', 'ForkEvent', 'CommitCommentEvent',
  'DeleteEvent', 'PullRequestEvent', 'PullRequestReviewCommentEvent',
  'GollumEvent', 'ReleaseEvent', 'MemberEvent', 'PublicEvent'
])
const passiveTypes = new Set(['IssuesEvent', 'IssueCommentEvent', 'WatchEvent'])

const logline = obj => {
  obj.quarter = quarter(new Date(obj.created_at))
  if (activeTypes.has(obj.type)) obj.grouping = 'active'
  else if (passiveTypes.has(obj.type)) obj.grouping = 'inactive'
  else {
    console.error(obj.type)
    obj.grouping = 'unknown'
  }
  for (let collection of Object.values(collections)) {
    if (collection.count) collection.count(obj)
    else if (collection.add) collection.add(obj)
  }
}

const processStream = async (parser, _logline = logline) => {
  let stream = parser.pipe(new PassThrough({ objectMode: true }))
  for await (let obj of stream) {
    await _logline(obj)
  }
}

const run = async (dataFile, outputDir, _logline = logline, _finish = finish) => {
  mkdirp.sync(outputDir)
  let parser = jsonstream.parse()
  let finished = processStream(parser, _logline)
  createReadStream(dataFile).pipe(parser)
  await finished
  _finish(outputDir)
}

// run('2018-pl-data', 'test-metrics')
module.exports = run
