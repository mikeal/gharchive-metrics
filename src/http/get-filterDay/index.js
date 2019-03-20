const gharchive = require('@architect/shared/gharchive')
const s3 = require('@architect/shared/s3')()
const lambda = require('@architect/shared/lambda')()

const pull = async (file, filter, cborSize) => {
  if (!await s3.hasObject(`gharchive/${file}`)) {
    let archive = await gharchive(file, 3)
    if (!archive) return { fail: { archive } }
  }
  return lambda('filter', { file, filter, cborSize })
}

const run = async (day, filter, cborSize) => {
  let i = 0
  let promises = []
  while (i < 24) {
    let file = day + '-' + i + '.json.gz'
    promises.push(pull(file, filter, cborSize))
    i += 1
  }
  let results = await Promise.all(promises)
  return results.map(ret => ret.cache)
}

exports.handler = async function http (req) {
  let { day, filter, cborSize } = req.query
  let ret = await run(day, filter, cborSize)
  return {
    type: 'application/json; charset=utf8',
    body: JSON.stringify(ret)
  }
}
