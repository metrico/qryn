import React from 'react'
import { NavLink } from 'react-router-dom'
import { ROUTE_PATH } from '../../constants'

export function AlertListItem ({ data, onDelete }) {
  function handleDelete () {
    onDelete({ name: data.name })
  }

  return (
    <div>
      <NavLink to={`${ROUTE_PATH.ALERT}/${data.name}`}>{data.name}</NavLink>
      <label>{JSON.stringify(data)}</label>
      <button onClick={handleDelete}>Delete</button>
    </div>
  )
}
