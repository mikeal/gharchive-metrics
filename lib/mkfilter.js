const cbor = require('dag-cbor-sync')()
const createS3 = require('../src/shared/s3')

const mkfilter = async (orgs, repos, profile) => {
  let s3 = createS3(profile)
  let block = await cbor.mkblock({orgs, repos})
  let result = await s3.storeBlock(block)
  return block
}

module.exports = mkfilter

