const path = require('path')
const { Worker } = require('worker_threads')

function runService() {
  const worker = new Worker(path.join(__dirname, 'download-service.js'));
  worker.pending = {}
  worker.on('message', resp => {
    worker.pending[resp.key](resp)
    delete worker.pending[resp.key]
  })
  worker.getKey = opts => {
    worker.postMessage(opts)
    return new Promise(resolve => {
      worker.pending[opts.key] = resp => {
        resp.service = worker
        resolve(resp)
      }
    })
  }
  return worker
}


exports.all = async (profile, keys, outputDir, limit=100, workers=3) => {
  const services = Array.isArray(workers) ? workers : []
  let serviceIndex = 0
  const _getKey = (opts) => {
    let ret = services[serviceIndex].getKey(opts)
    serviceIndex += 1
    if (serviceIndex === services.length) serviceIndex = 0
    return ret
  }

  const promises = new Set()
  const results = []
  const run = (service) => {
    if (!keys.length) return
    let key = keys.shift()
    let getKey = service ? service.getKey : _getKey
    let p = getKey({profile, key, outputDir})
    p.then(ret => {
      promises.delete(p)
      results.push(ret)
      run(ret.service)
    })
    promises.add(p)
  }
  if (!services.length) {
    let i = 0
    for (let i = 0; i < workers; i++) {
      services.push(runService())
    }
  }
  for (let i = 0; i < (workers * limit); i++) {
    run()
  }
  while (promises.size) {
    let info = await Promise.race(Array.from(promises))
    // console.error(info.filename)
  }
  results.services = services
  return results
}

