const bent = require('bent')

const run = async (day, url, filter, profile, outputFile) => {
  let get = bent(url + '/pull-day', 'json')
  let keys = await get(`?day=${day}&filter=${filter}`)
  console.error({keys})
  return keys
}

module.exports = run

