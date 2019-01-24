const s3 = require('./s3')
const bent = require('bent')
const url = require('url')
const querystring = require('querystring')
const { PassThrough } = require('stream')

const getFile = bent('http://data.gharchive.org/', {
  'User-Agent': 'ipfs-metrics-' + Math.random()
})

module.exports = async file => {
  const path = `gharchive/${file}`
  if (await s3.hasObject(path)) {
    return s3.getStream(path)
  } else {
    let f
    try {
      f = await getFile(file)
    } catch (e) {
      if (e.statusCode === 404) {
        console.error(file, 'is missing from gharchive, skipping.')
        return null
      } else {
        throw e
      }
    }

    f.pipe(s3.upload(path))
    return f
  }
}

