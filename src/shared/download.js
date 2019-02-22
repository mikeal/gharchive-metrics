const path = require('path')
const { Worker } = require('worker_threads')

function runService(workerData) {
  return new Promise((resolve, reject) => {
    const worker = new Worker(path.join(__dirname, 'download-service.js'), { workerData });
    worker.on('message', resolve);
    worker.on('error', reject);
    worker.on('exit', (code) => {
      if (code !== 0)
        reject(new Error(`Worker stopped with exit code ${code}`));
    })
  })
}

module.exports = (profile, key, outputDir) => runService({profile, key, outputDir})
module.exports.all = async (profile, keys, outputDir, limit=5) => {
  console.error({keys})
  process.exit()
  const promises = new Set()
  const results = []
  const run = () => {
    if (!keys.length) return
    let p = runService({profile, key: keys.shift(), outputDir})
    p.then(ret => {
      promises.delete(p)
      results.push(ret)
    })
    promises.add(p)
  }
  let i = 0
  while (keys.length && i < limit) {
    i += 1
    run() 
  }
  while (promises.size) {
    await Promise.race(Array.from(promises))
    run()
  }
  return results
}

