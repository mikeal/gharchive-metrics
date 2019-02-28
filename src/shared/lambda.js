const { Lambda } = require('aws-sdk')

module.exports = (profile, region = 'us-west-2') => {
  let lambda = new Lambda({ profile, region })
  return async (name, query) => {
    let env = process.env.NODE_ENV
    let FunctionName = `ghmetrics-${env || 'staging'}-get-${name}`
    let Payload = Buffer.from(JSON.stringify({ query }))
    let resp = await lambda.invoke({ FunctionName, Payload }).promise()
    if (resp.StatusCode !== 200) throw new Error(`Status not 200, ${resp.StatusCode}`)
    return JSON.parse(JSON.parse(resp.Payload).body)
  }
}
