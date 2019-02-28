const lambda = require('@architect/shared/lambda')()
const oneday = 1000 * 60 * 60 * 24

const pull = async (day, filter) => {
  return lambda('filterDay', { day, filter })
}

const flatten = arrays => [].concat.apply([], arrays)

const run = async (month, filter, limit = 940) => {
  let ts = new Date(month + '-01')
  let runningCost = 0
  let days = []
  let results = []
  month = ts.getMonth()
  while (ts.getMonth() === month) {
    let day = ts.toISOString().slice(0, 10)
    days.push(day)
    ts = new Date(ts.getTime() + oneday)
  }

  let _do = day => {
    let p = pull(day, filter)
    running.add(p)
    p.then(result => {
      results.push(result)
      running.delete(p)
      if (days.length) _do(days.shift())
    })
  }

  let running = new Set()
  while (runningCost < limit && days.length) {
    runningCost += (1 /* filterDay */ + (2 /* filter & pluck */ * 24 /* hours */))
    _do(days.shift())
  }
  while (running.size) {
    await Promise.race(Array.from(running))
  }
  return flatten(results)
}

exports.handler = async function http (req) {
  let { month, filter, limit } = req.query
  let ret = await run(month, filter, limit)
  return {
    type: 'application/json; charset=utf8',
    body: JSON.stringify(ret)
  }
}
