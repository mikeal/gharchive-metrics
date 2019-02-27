const path = require('path')
const fs = require('fs').promises
const createS3 = require('./s3')

const download = async opts => {
  let { profile, key, outputDir } = opts
  let s3 = createS3(profile)
  let buffer = await s3.getObject(key)
  let filename = key.slice(key.lastIndexOf('/'))
  await fs.writeFile(path.join(outputDir, filename), buffer)
  return { len: buffer.length, key: opts.key }
}

exports.all = async (profile, keys, outputDir, limit=1, workers=1) => {
  let count = 0
  let ret = { length: keys.length }

  while (keys.length) {
    let key = keys.shift()
    let { len } = await download({profile, key, outputDir})
    count += len
  }
  ret.size = count
  return ret
}

