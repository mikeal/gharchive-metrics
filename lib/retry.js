const bent = require('bent')

const make = (...args) => {
  const __retry = (...args) => {
    let get = bent(...args)
    const _retry = async (retry, ...args) => {
      let ret = null
      try {
        ret = await get(...args)
      } catch (e) {
        if (retry > 3) throw e
        return _retry(retry+1, ...args)
      }
      return ret
    }
    return (...args) => _retry(1, ...args)
  }
  return __retry(...args)
}

module.exports = make

