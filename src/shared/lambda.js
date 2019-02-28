const { Lambda } = require('aws-sdk')

class LambdaError extends Error {
  constructor(data) {
    let msg = data.errorMessage
    msg = `\nLambdaError[${data.errorType}]: ` + data.errorMessage
    msg += '\n'
    msg += data.stackTrace.map(s => '  ' + s).join('\n')
    msg += '\nError[local]: local stack'
    super(msg)
  }
}

module.exports = (profile, region = 'us-west-2') => {
  let lambda = new Lambda({ profile, region })
  return async (name, query) => {
    let env = process.env.NODE_ENV
    let FunctionName = `ghmetrics-${env || 'staging'}-get-${name}`
    let Payload = Buffer.from(JSON.stringify({ query }))
    let resp = await lambda.invoke({ FunctionName, Payload }).promise()
    if (resp.StatusCode !== 200) throw new Error(`Status not 200, ${resp.StatusCode}`)
    let data = JSON.parse(resp.Payload)
    if (data.errorMessage) throw new LambdaError(data)
    return JSON.parse(data.body)
  }
}
