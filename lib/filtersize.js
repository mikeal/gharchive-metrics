const createS3 = require('../src/shared/s3')
const cbor = require('dag-cbor-sync')(655360)

const run = async (hash, profile) => {
  hash = 'zdpuArsRqf65GwPmCVP1weR6NXAa9sFhosrx8ruX6S2pnrdMH'
  profile = 'pl'
  let s3 = createS3(profile)
  let { size } = await s3.hasObject(`blocks/${hash}`)
  console.log(`Block Size: ${size}`)
  let { orgs, repos, keys } = cbor.deserialize(await s3.getObject(`blocks/${hash}`))
  console.log({orgs: orgs.length, repos: repos.length, keys: keys.length})
}
run()
