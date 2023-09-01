const path = require('path');
const nodeExternals = require('webpack-node-externals');

module.exports = {
  entry: './src/index_ssl_qa.ts',
  target: 'node',
  module: {
    rules: [
      {
        test: /\.tsx?$/,
        use: 'ts-loader',
        exclude: /node_modules/,
      },
    ],
  },
  resolve: {
    extensions: ['.ts', '.js'],
  },
  output: {
    filename: 'qaweeklypay-api-main.js',
    path: path.resolve(__dirname, 'distqa'),
  },
  mode:"production",
  externals: [ nodeExternals() ]
};
