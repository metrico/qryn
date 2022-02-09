module.exports = {
  apps: [{
    name: 'cloki',
    script: './cloki.js',
    instances : "max",
    exec_mode : "cluster"
  }]
}
