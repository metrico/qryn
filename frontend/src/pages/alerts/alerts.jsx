import React, { useState, useEffect } from 'react'
import { Page } from '../page'
import { AlertList } from './alert_list'
import { AlertService } from '../../services'
import { apiErrorToString } from '../../lib'

export function Alerts () {
  const alertService = new AlertService()

  const [error, setError] = useState(null)
  const [isLoading, setIsLoading] = useState(false)
  const [alerts, setAlerts] = useState([])
  const [alertsCount, setAlertsCount] = useState(0)
  const [alertsOffset] = useState(0)
  const [alertsLimit] = useState(10)

  function preRequestState () {
    setError(null)
    setIsLoading(true)
  }

  function postRequestState () {
    setIsLoading(false)
  }

  function handleRequestError (error, fnName) {
    console.error(fnName, error)
    setError(apiErrorToString(error))
  }

  async function heandleNextPageAlerts () {
    if (alerts.length < alertsCount) {
      getAlerts({ offset: alertsOffset + alertsLimit })
    }
  }

  async function handlePrevPageAlerts () {
    let offset = alertsOffset - alertsLimit
    if (offset < 0) offset = 0
    getAlerts({ offset })
  }

  async function getAlerts ({ offset = alertsOffset, limit = alertsLimit } = {}) {
    preRequestState()

    try {
      const { data } = await alertService.getAlerts({ offset, limit })
      console.debug('getAlerts', data)
      setAlerts(data.alerts.sort((a, b) => a.name.localeCompare(b.name)))
      setAlertsCount(data.count)
    } catch (error) {
      handleRequestError(error, 'getAlerts')
    }

    postRequestState()
  }

  async function handleDeleteAlert ({ name }) {
    preRequestState()

    try {
      const { data } = await alertService.deleteAlert({ name })
      console.debug('deleteAlert', data)
      getAlerts()
    } catch (error) {
      handleRequestError(error, 'deleteAlert')
    }

    postRequestState()
  }

  useEffect(() => {
    getAlerts()
  }, [])

  return (
    <Page error={error} isLoading={isLoading} header="Alerts">
      <AlertList data={alerts} onDelete={handleDeleteAlert} />
      <button onClick={handlePrevPageAlerts}>Prev page</button>
      <button onClick={heandleNextPageAlerts}>Next page</button>
    </Page>
  )
}
