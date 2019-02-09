// @architect/functions enables secure sessions, express-style middleware and more
// let arc = require('@architect/functions')
// let url = arc.http.helpers.url

let gharchive = require('./lib/gharchive')

let json = obj => JSON.stringify(obj, null, 2)

exports.handler = async function http(req) {
  let type = 'text/html; charset=utf8'
  let file = req.query.file
  if (!file) {
    return {type, body: '<h1>Wrong params :(</h1>'}
  }
  let stored = await gharchive(file)
  return {
    type: 'application/json',
    body: json({file, stored})
  }
}

// Example responses

/* Forward requester to a new path
exports.handler = async function http(request) {
  if (process.env.NODE_ENV !== 'production') {
    console.log(request)
  }
  return {
    status: 302,
    location: '/staging/about',
  }
}
*/

/* Successful resource creation, CORS enabled
exports.handler = async function http(request) {
  return {
    status: 201,
    type: 'application/json',
    body: JSON.stringify({ok: true}),
    cors: true,
  }
}
*/

/* Deliver client-side JS
exports.handler = async function http(request) {
  return {
    type: 'text/javascript',
    body: 'console.log("Hello world!")',
  }
}
*/

// Learn more: https://arc.codes/guides/http
