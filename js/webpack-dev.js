var merge = require("webpack-merge").merge;

var generatedConfig = require('./scalajs.webpack.config');
var commonConfig = require("./webpack-common.js");

module.exports = merge(generatedConfig, commonConfig, {

    // devtool: 'source-map',
    mode: 'development',
    module: {
        rules: [
            {
                test: /\.(jpe?g|png|gif|svg)$/i,
                use: [
                    {
                        loader: "file-loader",
                        options: {
                            hash: "sha512",
                            digest: "hex",
                            name: "[hash].[ext]"
                        }
                    }
                ]
            },
         ],
        noParse: function (content) {
            return content.endsWith("-fastopt.js");
        }
    }
});
