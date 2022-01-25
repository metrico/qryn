const { CLokiNotFound, CLokiError } = require('../handlers/errors')
const { formatISO9075 } = require('date-fns')
const CLokiClient = require('../db/clickhouse').client
const isEnabled = () => (process.env.MULTITENANCY || '').toUpperCase() === 'ON'
let orgs = {}
let updater = null
/**
 *
 * @returns {Promise<CLokiClient>}
 */
module.exports.getClient = async (orgid) => {
  if (!isEnabled()) {
    return new CLokiClient()
  }
  if (!orgid) {
    return new MultitenantClient()
  }
  if (!orgs[orgid]) {
    throw new CLokiNotFound(`Organization ${orgid} not exists`)
  }
  return new MultitenantClient({ db: orgs[orgid].db })
}

module.exports.init = () => {
  if (!isEnabled()) {
    return
  }
  if (updater) {
    return
  }
  updater = setInterval(async () => {
    try {
      orgs = await (new MultitenantClient()).getAllOrgs()
    } catch (e) {
      console.log(e)
    }
  }, 30000);
  (new MultitenantClient()).getAllOrgs().then((res) => { orgs = res }, console.log)
}

module.exports.registerFastify = (fastify) => {
  return fastify.decorateRequest('client', function () {
    if (!isEnabled()) {
      return module.exports.getClient()
    }
    if (!this.headers || !this.headers['x-scope-orgid']) {
      throw new CLokiError(400, 'Bad request', 'The OrgID header is required')
    }
    return module.exports.getClient(this.headers['x-scope-orgid'])
  })
}

module.exports.stop = () => {
  if (!updater) {
    return
  }
  clearInterval(updater)
  updater = null
}

class MultitenantClient extends CLokiClient {
  /**
   * @returns {Promise<Object<string, Tenant>>}
   */
  async getAllOrgs () {
    const resp = await this.rawRequest(`SELECT DISTINCT argMax(name, inserted_at) as name, 
          argMax(value, inserted_at) as db
        FROM settings
        WHERE type = 'orgID'
        GROUP BY fingerprint
        HAVING name != '' FORMAT JSON`,
    null, null)
    if (!resp.data || !resp.data.data || !resp.data.data.length) {
      return {}
    }
    return resp.data.data.reduce((sum, r) => {
      sum[r.name] = JSON.parse(r.db)
      return sum
    }, {})
  }

  /**
   *
   * @param org {string}
   * @param dbname {string}
   * @param samplesDays {number}
   * @param timeSeriesDays {number}
   * @param storagePolicy {string}
   * @returns {Promise<void>}
   */
  async addTenant (org, dbname, samplesDays, timeSeriesDays, storagePolicy) {
    const tenant = {
      db: dbname,
      samples_days: samplesDays,
      time_series_days: timeSeriesDays,
      storagePolicy: storagePolicy
    }
    await this.rawRequest('INSERT INTO settings (fingerprint, type, name, value, inserted_at) FORMAT JSONEachRow',
      JSON.stringify({
        fingerprint: this.getFP('orgID', org),
        type: 'orgID',
        name: org,
        value: JSON.stringify(tenant),
        inserted_at: formatISO9075(new Date())
      }) + '\n')
    orgs[org] = tenant
  }
}
