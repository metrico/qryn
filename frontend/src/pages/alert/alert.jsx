import React, { useEffect, useState } from 'react'
import { useParams, NavLink, useNavigate } from 'react-router-dom'
import { Formik } from 'formik'
import { Page } from '../page'
import { ROUTE_PATH } from '../../constants'
import { AlertService } from '../../services'
import { apiErrorToString } from '../../lib'
import { getFormikError } from '../../lib/formik'
import { FormikInputError } from '../../components'

export const DEFAULT_ALERT = {
  name: '',
  request: '',
  labels: ''
}

export function alertToFormik (alert) {
  const formik = { ...alert }
  formik.labels = JSON.stringify(formik.labels)
  return formik
}

export function formikToAlert (formik) {
  const alert = { ...formik }
  if (alert.labels === '') {
    delete alert.labels
    return alert
  }

  try {
    alert.labels = JSON.parse(alert.labels)
    return alert
  } catch (error) {
    throw new Error('Fail to transform form data to alert', error)
  }
}

export function validateForm (values) {
  const errors = {}
  if (!values.name) errors.name = 'Required'
  if (!values.request) errors.request = 'Required'
  return errors
}

export function AlertName ({ value, onChange, onBlur }) {
  return (
    <>
      <label htmlFor="name">Name</label>
      <br />
      <input
        type="text"
        name="name"
        id="name"
        value={value}
        onChange={onChange}
        onBlur={onBlur}
      />
    </>
  )
}

export function AlertRequest ({ value, onChange, onBlur }) {
  return (
    <>
      <label htmlFor="request">Request</label>
      <br />
      <input
        type="text"
        name="request"
        id="request"
        value={value}
        onChange={onChange}
        onBlur={onBlur}
      />
    </>
  )
}

export function AlertLabels ({ value, onChange, onBlur }) {
  return (
    <>
      <label htmlFor="labels">Labels</label>
      <br />
      <input
        type="text"
        name="labels"
        id="labels"
        disabled={true}
        value={value}
        onChange={onChange}
        onBlur={onBlur}
      />
    </>
  )
}

export function Alert () {
  const alertService = new AlertService()
  const navigate = useNavigate()
  const params = useParams()

  const [error, setError] = useState(null)
  const [isLoading, setIsLoading] = useState(false)
  const [alert, setAlert] = useState(DEFAULT_ALERT)

  function preRequestState () {
    setError(null)
    setIsLoading(true)
  }

  function postRequestState ({ setSubmitting, navigateTo } = {}) {
    setIsLoading(false)
    if (setSubmitting) setSubmitting(false)
    if (navigateTo && !error) navigate(navigateTo)
  }

  function handleRequestError (error, fnName) {
    console.error(fnName, error)
    setError(apiErrorToString(error))
  }

  async function addAlert (values, { navigateTo = null, setSubmitting } = {}) {
    preRequestState()
    try {
      const { data } = await alertService.addAlert(formikToAlert(values))
      console.debug('addAlert', data)
    } catch (error) {
      handleRequestError(error, 'addAlert')
    }
    postRequestState({ setSubmitting, navigateTo })
  }

  async function updateAlert (values, { navigateTo = null, setSubmitting } = {}) {
    preRequestState()
    try {
      const { data } = await alertService.updateAlert(formikToAlert(values))
      console.debug('updateAlert', data)
    } catch (error) {
      handleRequestError(error, 'updateAlert')
    }
    postRequestState({ setSubmitting, navigateTo })
  }

  function handleAddAlert (values, { setSubmitting } = {}) {
    const doUpdate = params?.alertName && params.alertName === values.name
    const navigateTo = ROUTE_PATH.ALERTS

    if (doUpdate) updateAlert(values, { navigateTo, setSubmitting })
    else addAlert(values, { navigateTo, setSubmitting })
  }

  async function getAlert ({ name }) {
    preRequestState()

    try {
      const { data } = await alertService.getAlert({ name })
      console.debug('getAlert', data)

      setAlert(alertToFormik(data))
    } catch (error) {
      handleRequestError(error, 'getAlert')
    }

    postRequestState()
  }

  useEffect(() => {
    if (params?.alertName) getAlert({ name: params.alertName })
  }, [])

  return (
    <Page header="Alert" error={error} isLoading={isLoading}>
      <NavLink to={ROUTE_PATH.ALERTS}>Back</NavLink>
      <Formik
        enableReinitialize={true}
        initialValues={alert}
        onSubmit={handleAddAlert}
        validate={validateForm}
      >
        {({
          values,
          errors,
          touched,
          handleChange,
          handleBlur,
          handleSubmit,
          isSubmitting
        }) => {
          return (
            <form onSubmit={handleSubmit}>
              <AlertName value={values.name} onChange={handleChange} onBlur={handleBlur} />
              <FormikInputError error={getFormikError({ errors, touched, inputName: 'name' })} />

              <AlertRequest value={values.request} onChange={handleChange} onBlur={handleBlur} />
              <FormikInputError error={getFormikError({ errors, touched, inputName: 'request' })} />

              <AlertLabels value={values.labels} onChange={handleChange} onBlur={handleBlur} />
              <FormikInputError error={getFormikError({ errors, touched, inputName: 'labels' })} />

              <button type="submit" disabled={isSubmitting}>Save</button>
            </form>
          )
        }}
      </Formik>
    </Page>
  )
}
