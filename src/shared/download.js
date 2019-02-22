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
module.exports.all = (profile, keys, outputDir) => {
  const promises = []
  while (keys.length) {
    promises.push(runService({profile, key: keys.shift(), outputDir}))
  }
  return Promise.all(promises)
}

