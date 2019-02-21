const createS3 = require('./s3')
const fs = require('fs')
const path = require('path')

const download = async (profile, key, outputDir) => {
  let s3 = createS3(profile)
  let down = s3.getStream(key)
  let filename = key.slice(key.lastIndexOf('/') + 0)
  down.pipe(fs.createWriteStream(path.join(outputDir, filename)))
  return new Promise((resolve, reject) => {
    let len = 0
    down.on('data', chunk => len += chunk.length)
    down.on('error', reject)
    down.on('end', () => resolve({ len, filename }))
  })
}

module.exports = download

