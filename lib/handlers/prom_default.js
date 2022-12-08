/* Emulated PromQL Query Handler */

async function handler (req, res) {
  const path = req.url
  req.log.debug('PROM Handler',path)
  switch (path) {
    case '/api/v1/status/buildinfo':
      res.send(
        { "status": "success", 
           "data": { 
             "version": "2.13.1",
             "revision": "cb7cbad5f9a2823a622aaa668833ca04f50a0ea7",
             "branch": "master",
             "buildUser": "qryn@qxip",
             "buildDate": "29990401-13:37:420",
             "goVersion": "go1.18.1"
           }
        })
      break
    case '/api/v1/rules':
      res.send({
        "status": "success", 
        "data": {
          "groups": [
              {
                  "rules": []
              }
            ]
        }
      })
    default:
      res.send({"status": "success", "data": {}})
  }
  return
}

module.exports = handler
