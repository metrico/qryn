const { PluginLoaderBase } = require('plugnplay')

module.exports = class extends PluginLoaderBase {
  exportSync (options) {
    return {
      label_to_row: {
        /**
         *
         * @param parameters {string[]}
         */
        remap: (parameters) => {
          const labelsToRemap = parameters.length
            ? JSON.parse(parameters[0]).split(',').map(p => p.trim())
            : undefined
          return (emit, entry) => {
            if (labelsToRemap) {
              for (const l of labelsToRemap) {
                if (entry.labels[l]) {
                  const rm = {
                    ...entry,
                    labels: { label: l },
                    string: entry.labels[l]
                  }
                  emit(rm)
                }
              }
              return
            }
            for (const [l, v] of Object.entries(entry)) {
              emit({
                ...entry,
                labels: { label: l },
                string: v
              })
            }
          }
        }
      }
    }
  }
}
