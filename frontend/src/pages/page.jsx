import React from 'react'

export function Page ({ error, isLoading, header, children }) {
  return (
    <div>
      {isLoading && <h2>Is loading ...</h2>}
      {error && <h2 style={{ color: 'red' }}>{error}</h2>}
      <h2>{header}</h2>

      <div>
        {children}
      </div>
    </div>
  )
}
