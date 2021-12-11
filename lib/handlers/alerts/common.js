/**
 * @param ns {Object<string, alerting.objGroup>}
 * @returns {alerting.group[]}
 */
module.exports.nsToResp = (ns) => {
  return Object.values(ns).map(module.exports.groupToResp)
}

/**
 * @param grp {alerting.objGroup}
 * @returns {alerting.group}
 */
module.exports.groupToResp = (grp) => {
  return {
    ...grp,
    rules: Object.values(grp.rules).map(r => {
      const _r = { ...r }
      delete _r._watcher
      return _r
    })
  }
}
