const cbor = require('dag-cbor-sync')()
const createS3 = require('../src/shared/s3')
const mkQuery = require('./query')

const filter = async (profile, timerange, url, parallelism, filter) => {
  let s3 = createS3(profile)
  let { orgs, repos, keys } = cbor.deserialize(await s3.getObject(`blocks/${filter}`))
  keys.push('repo.name')
  keys.push('created_at')
  keys = Array.from(new Set(keys))
  keys = keys.map(k => 's.' + k).join(', ')
  let sql = `SELECT ${keys} from S3Object s`
  let query = mkQuery(profile, sql, timerange, url, parallelism)
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
