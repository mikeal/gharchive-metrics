const lambda = require('./lambda')()

const gharchive = async (file, retry = 0, log = console.log) => {
  var stored
  try {
    var { stored } = await lambda('fetch', { file })
  } catch (e) {
    log(`Retrying ${file}`)
    if (retry > 3) throw e
    return gharchive(file, retry + 1, log)
  }
  return stored
}

module.exports = gharchive
