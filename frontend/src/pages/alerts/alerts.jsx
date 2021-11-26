import React, { useState, useEffect, useMemo } from 'react'
import { debounce } from 'lodash'
import { Page } from '../page'
import { AlertList } from './alert_list'
import { AlertService } from '../../services'
import { apiErrorToString } from '../../lib'

export function AlertSearchInput ({ input, onChange }) {
  function handleInputChange (e) {
    onChange(e.target.value)
  }

  return (
    <div>
      <label htmlFor="search">Search</label>
      <br />
      <input
        type="text"
        name="search"
        id="search"
        value={input}
        onChange={handleInputChange}
        style={{ width: 400 }}
      />
    </div>
  )
}

export function Alerts () {
  const alertService = new AlertService()

  const [error, setError] = useState(null)
  const [isLoading, setIsLoading] = useState(false)
  const [alerts, setAlerts] = useState([])
  const [alertsCount, setAlertsCount] = useState(0)
  const [alertsOffset] = useState(0)
  const [alertsLimit] = useState(10)
  const [searchInput, setSerchInput] = useState('')

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
      getAlerts({ offset: alertsOffset + alertsLimit, alertName: searchInput })
    }
  }

  async function handlePrevPageAlerts () {
    let offset = alertsOffset - alertsLimit
    if (offset < 0) offset = 0
    getAlerts({ offset })
  }

  async function getAlerts ({ offset = alertsOffset, limit = alertsLimit, alertName = searchInput } = {}) {
    console.log('---------------- getAlerts', offset, limit, alertName, searchInput)
    preRequestState()
    try {
      const { data } = await alertService.getAlerts({ offset, limit, alertName })
      console.debug('getAlerts', data)
      setAlerts(data.alerts.sort((a, b) => a.name.localeCompare(b.name)))
      setAlertsCount(data.count)
    } catch (error) {
      handleRequestError(error, 'getAlerts')
    }
    postRequestState()
  }

  const debounceGetAlerts = useMemo(() => {
    return debounce((args) => {
      return getAlerts(args)
    }, 1000)
  }, [])

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

  async function handleSearch (alertName) {
    setSerchInput(alertName)
  }

  useEffect(() => {
    getAlerts()
  }, [])

  useEffect(() => {
    debounceGetAlerts({ alertName: searchInput })
    return () => {
      debounceGetAlerts.cancel()
    }
  }, [searchInput])

  return (
    <Page error={error} isLoading={isLoading} header="Alerts">
      <AlertSearchInput input={searchInput} onChange={handleSearch} />
      <AlertList data={alerts} onDelete={handleDeleteAlert} />
      <button onClick={handlePrevPageAlerts}>Prev page</button>
      <button onClick={heandleNextPageAlerts}>Next page</button>
    </Page>
  )
}
