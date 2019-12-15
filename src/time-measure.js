"use strict"

const measure = {
  config(options) {
    for (let prop in options) {
      this[prop] = options[prop]
    }
  },
  start() {
    if (!this.measureExecutionTime) { return }
    return process.hrtime()
  },
  end(t, info) {
    if (!this.measureExecutionTime) { return }
    const hrend = process.hrtime(t)
    console.log('\n-----------------------------------------------------------')
    console.log(`-  ${info}`)
    console.log('-  Execution time (hr): %ds %dms', hrend[0], hrend[1] / 1000000)
    console.log('-----------------------------------------------------------')
    this._hstart = null
  }
}
module.exports = {
  measure
}
