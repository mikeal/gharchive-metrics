const gharchive = require('./lib/gharchive')

const run = async () => {
  let result = await gharchive('2019-01-01-15.json.gz')
  console.log({ result })
}
run()
