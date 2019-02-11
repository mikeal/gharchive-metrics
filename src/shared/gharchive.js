const bent = require('bent')

const gharchive = async (file, url, retry=0, log=console.log) => {
  let get = bent(url + '/fetch', 'json')
  var stored
  try {
    var { stored } = await get(`?file=${file}`)
  } catch (e) {
    log(`Retrying ${file}`)
    if (retry > 3) throw e
    return gharchive(file, url, retry+1, log)
  }
  return stored
}

module.exports = gharchive

