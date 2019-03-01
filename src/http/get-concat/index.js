const s3 = require('@architect/shared/s3')()
const crypto = require('crypto')

const md5 = b => crypto.createHash('sha256').update(b).digest('hex')

const merge = async files => {
  let promises = new Set(files.map(f => s3.getObject(f)))
  for (let p of promises.values()) {
    p.then(() => promises.delete(p))
  }
  let hash = md5(Buffer.from(JSON.stringify(files)))
  let cachekey = `cache/concat/${hash}`
  let upload = s3.upload(cachekey)
  while (promises.size) {
    let buffer = await Promise.race(Array.from(promises))
    upload.write(buffer)
  }
  let finish = new Promise(resolve => upload.on('finish', () => resolve(cachekey)))
  upload.end()
  return finish
}

exports.handler = async function http (req) {
  const { files } = req.query
  let cachekey = await merge(files)
  return {
    type: 'application/json; charset=utf8',
    body: JSON.stringify({ cache: cachekey })
  }
}

