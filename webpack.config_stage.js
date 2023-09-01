const path = require('path');
const nodeExternals = require('webpack-node-externals');

module.exports = {
  entry: './src/index_ssl_stage.ts',
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
    filename: 'stageweeklypay-api-main.js',
    path: path.resolve(__dirname, 'diststage'),
  },
  mode:"production",
  externals: [ nodeExternals() ]
};
