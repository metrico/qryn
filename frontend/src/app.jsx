import React from 'react'
import { Outlet, Link } from 'react-router-dom'
import { ROUTE_PATH } from './constants'

export function App () {
  return (
    <div>
      <header>
        <h1>cLoki</h1>
      </header>

      <nav
        style={{
          borderBottom: 'solid 1px',
          paddingBottom: '1rem'
        }}
      >
        <Link to={ROUTE_PATH.ALERTS}>Alerts</Link>
      </nav>

      <div>
        <Outlet />
      </div>
    </div>
  )
}
