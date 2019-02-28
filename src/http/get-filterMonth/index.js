const lambda = require('@architect/shared/lambda')()
const oneday = 1000 * 60 * 60 * 24

const pull = async (day, filter) => {
  return lambda('filterDay', { day, filter })
}

const flatten = arrays => [].concat.apply([], arrays)

const run = async (month, filter) => {
  let promises = []
  let ts = new Date(month + '-01')
  month = ts.getMonth()
  while (ts.getMonth() === month) {
    let day = ts.toISOString().slice(0, 10)
    console.error({ day })
    let p = pull(day, filter)
    promises.push(p)
    p.then(() => console.log({ finish: day }))
    ts = new Date(ts.getTime() + oneday)
  }
  let results = await Promise.all(promises)
  return flatten(results)
}

exports.handler = async function http (req) {
  let { month, filter } = req.query
  let ret = await run(month, filter)
  return {
    type: 'application/json; charset=utf8',
    body: JSON.stringify(ret)
  }
}
