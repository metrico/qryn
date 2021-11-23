import { API_PATH } from '../constants'
import { httpClient } from './http_client'

export class AlertService {
  async addAlert ({ name, request, labels } = {}) {
    const body = { name, request }
    if (labels) body.labels = labels

    return await httpClient.post(API_PATH.ALERTS, body)
  }

  async getAlerts ({ offset = 0, limit = 10 } = {}) {
    const url = new URL(`http://placeholder/${API_PATH.ALERTS}`)
    if (Number.isInteger(offset)) url.searchParams.set('offset', offset)
    if (Number.isInteger(limit)) url.searchParams.set('limit', limit)

    return await httpClient.get(url.pathname + url.search)
  }

  async getAlert ({ name } = {}) {
    return await httpClient.get(API_PATH.ALERTS + `/${name}`)
  }

  async updateAlert ({ name, request, labels } = {}) {
    const body = { name }
    if (request) body.request = request
    if (labels) body.labels = labels

    return await httpClient.put(API_PATH.ALERTS + `/${name}`, body)
  }

  async deleteAlert ({ name }) {
    return await httpClient.delete(API_PATH.ALERTS + `/${name}`)
  }
}
