const cbor = require('dag-cbor-sync')()
const createS3 = require('./s3')
const mkQuery = require('./query')

const filter = async (profile, sql, timerange, url, parallelism, filter) => {
  let s3 = createS3(profile)
  let query = mkQuery(profile, sql, timerange, url, parallelism)
  let {orgs, repos} = cbor.deserialize(await s3.getObject(`blocks/${filter}`))
  let _f = r => {
    if (r.name) {
      // has a repo name
      for (let repo of repos) {
        if (r.name === repo) return true
      }
      for (let org of orgs) {
        if (r.name.startsWith(`${org}/`)) return true
      }
    }
  }
  for await (let r of query) {
    if (_f(r)) {
      console.log(r)
    }
  }
}

module.exports = filter

