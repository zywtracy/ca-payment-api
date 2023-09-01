const path = require('path');
const nodeExternals = require('webpack-node-externals');

module.exports = {
  entry: './src/index_ssl_prod.ts',
  // entry: './src/index.ts',
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
    filename: 'prodweeklypay-api-main.js',
    path: path.resolve(__dirname, 'distprod'),
  },
  mode:"production",
  externals: [ nodeExternals() ]
};
