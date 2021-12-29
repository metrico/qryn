const axios = require('axios')
const format = require('date-fns/formatRFC3339')
/**
 * @param name {string}
 * @param alerts {{
 *   labels: Object<string, string>,
 *   annotations: Object<string, string>,
 *   message: string,
 *   start? : number | undefined,
 *   end?: number | undefined
 * }[]}
 */
const alert = async (name, alerts) => {
  if (!process.env.ALERTMAN_URL) {
    return
  }
  try {
    await axios.post(process.env.ALERTMAN_URL + '/api/v2/alerts', alerts.map(e => ({
      labels: {
        alertname: name,
        ...e.labels
      },
      annotations: { ...e.annotations, message: e.message },
      startsAt: e.start ? format(e.start) : undefined,
      endsAt: e.end ? format(e.end) : undefined
    })))
  } catch (e) {
    throw new Error(e.message + (e.response.data ? '\n' + JSON.stringify(e.response.data) : ''))
  }
}

module.exports = {
  alert
}
