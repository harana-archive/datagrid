const path = require("path");
const CopyWebpackPlugin = require('copy-webpack-plugin');
var HardSourceWebpackPlugin = require('hard-source-webpack-plugin');

module.exports = {
    resolve: {
        extensions: ['.ts', '.tsx', '.js', '.jsx'],
        modules: ['node_modules', '../main', '../main/node_modules'],
        alias: {
            react: path.resolve('../main/node_modules/react')
        }
    },
    
    module: {
        rules: [
            {
                test: /\.tsx?$/,
                loader: "awesome-typescript-loader?{configFileName: '../../../../../tsconfig.json'}",
            },
            {
                test: /\.css$/,
                use: ["style-loader", "css-loader"]
            },
            {
                test: /\.(png|jpg|gif|svg|eot|ttf|woff|woff2)$/,
                loader: "url-loader",
                options: {
                    limit: 10000
                }
            }
        ]
    },
    node: {
        fs: "empty"
    },
    output: {
        pathinfo: false
    },
    externals: {
        jquery: 'var jQuery'
    }
};