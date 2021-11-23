import React from 'react'
import { AlertListItem } from './alert_list_item'
import { NavLink } from 'react-router-dom'
import { ROUTE_PATH } from '../../constants'

export function AlertList ({ data, onDelete }) {
  return (
    <div>
      <NavLink to={ROUTE_PATH.ALERT} >Create alert</NavLink>
      {data.map((alert, i) => <AlertListItem key={i} data={alert} onDelete={onDelete} />)}
    </div>
  )
}
