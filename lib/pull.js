const bent = require('bent')
const mkdirp = require('mkdirp')
const fs = require('fs')
const path = require('path')
const createS3 = require('../src/shared/s3')
const gharchive = require('../src/shared/gharchive')

let key = `cache/zdpuAzeoMd6PEJ3A3CRYF4KMSB8PoZGC6jsx7K9RhtVs6fyDa/2019-01-01-15.json.gz`

const download = async (profile, key, outputDir) => {
  let s3 = createS3(profile)
  let down = s3.getStream(key)
  down.pipe(process.stdout)
  mkdirp.sync(outputDir)
  let filename = key.slice(key.lastIndexOf('/')+0)
  down.pipe(fs.createWriteStream(path.join(outputDir, filename)))
}

const pull = async (file, url, filter, profile, outputDir) => {
  let archive = await gharchive(file, url, retry=0, log=console.log)
  if (!archive) return {fail: {archive}}
  let get = bent(url + '/filter', 'json')
  let result = await get(`?file=${file}&filter=${filter}`)
  download(profile, result.cache, outputDir)
}

// download('pl', key, 'test-output')
const pkg = require('../package.json')
pull('2019-01-01-12.json.gz', pkg.productionURL, 'zdpuAzeoMd6PEJ3A3CRYF4KMSB8PoZGC6jsx7K9RhtVs6fyDa', 'pl', 'test-output')

