const createS3 = require('./s3')
const fs = require('fs')
const path = require('path')
const { createGunzip } = require('zlib')

const { parentPort } = require('worker_threads')

const download = async (opts) => {
  let { profile, key, outputDir } = opts
  let s3 = createS3(profile)
  let down = s3.getStream(key)
  let filename = key.slice(key.lastIndexOf('/') + 0, key.lastIndexOf('.gz'))
  down
    .pipe(createGunzip())
    .pipe(fs.createWriteStream(path.join(outputDir, filename)))
  return new Promise((resolve, reject) => {
    let len = 0
    down.on('data', chunk => len += chunk.length)
    down.on('error', reject)
    down.on('end', () => resolve({ len, filename, key: opts.key }))
  })
}

parentPort.on('message', async opts => {
  parentPort.postMessage(await download(opts))
})

