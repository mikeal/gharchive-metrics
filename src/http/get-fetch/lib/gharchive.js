const createS3 = require('@architect/shared/s3')
const bent = require('bent')
const url = require('url')
const querystring = require('querystring')
const { PassThrough } = require('stream')

const getFile = bent('http://data.gharchive.org/', {
  'User-Agent': 'gharchive-metrics-' + Math.random()
})

module.exports = async (file, ...args) => {
  const path = `gharchive/${file}`
  const s3 = createS3(...args)
  let _has = await s3.hasObject(path)
  if (_has) {
    return _has
  } else {
    let f
    try {
      f = await getFile(file)
    } catch (e) {
      if (e.statusCode === 404) {
        console.error(file, 'is missing from gharchive, skipping.')
        return false
      } else {
        throw e
      }
    }

    let upload = f.pipe(s3.upload(path))
    return new Promise((resolve, reject) => {
      upload.on('finish', () => resolve(true))
      upload.on('error', reject)
    })
  }
}

