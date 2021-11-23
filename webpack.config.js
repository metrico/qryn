require('dotenv').config({ path: './.env' })
const HtmlWebpackPlugin = require('html-webpack-plugin')
const path = require('path')

const PORT = process.env.DEV_SERVER_PORT
const API_PROXY_URL = process.env.DEV_API_PROXY_URL

module.exports = {
  entry: './frontend/src/index.js',
  output: {
    filename: 'bundle.[hash].js',
    path: path.resolve(__dirname, './frontend/dist')
  },
  plugins: [
    new HtmlWebpackPlugin({
      template: './frontend/public/index.html'
    })
  ],
  resolve: {
    modules: [__dirname, 'frontend/src', 'node_modules'],
    extensions: ['*', '.js', '.jsx', '.tsx', '.ts']
  },
  module: {
    rules: [
      {
        test: /\.jsx?$/,
        exclude: /node_modules/,
        loader: require.resolve('babel-loader')
      },
      {
        test: /\.css$/,
        use: ['style-loader', 'css-loader']
      },
      {
        test: /\.png|svg|jpg|gif$/,
        use: ['file-loader']
      }
    ]
  },
  devServer: {
    historyApiFallback: true,
    static: {
      directory: path.join(__dirname, './frontend/public')
    },
    compress: true,
    port: PORT,
    proxy: {
      '/config': API_PROXY_URL
    }
  }
}
