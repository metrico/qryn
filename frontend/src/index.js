import React from 'react'
import ReactDOM from 'react-dom'
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom'
import { App } from './app'
import { Alerts, Alert, Empty } from './pages'
import { ROUTE_PATH } from './constants'

/*
TODO
- [x] Routing.
- [] Alerts fetch pagination.
- [] Validate form in the DefineAlert, for example, formik.
- [] A couple of tests.
*/
ReactDOM.render(
  <BrowserRouter>
    <Routes>
      <Route path={ROUTE_PATH.HOME} element={<App />}>
        <Route path={ROUTE_PATH.ALERTS} element={<Alerts />}>
          <Route path="*" element={<Empty />} />
        </Route>
        <Route path={ROUTE_PATH.ALERT} element={<Alert />}>
          <Route path=":alertName" element={<Alert />} />
          <Route path="*" element={<Empty />} />
        </Route>
        <Route path="*" element={<Empty />} />
      </Route>
      <Route path="/" element={<Navigate to={ROUTE_PATH.HOME} />} />
    </Routes>
  </BrowserRouter>
  , document.querySelector('#root')
)
