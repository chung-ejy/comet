module.exports = {
    module: {
        rules : [
            {
                test:/\.js$/,
                exclude: /node_modules/,
                use: {
                    loader:"babel-loader"
                }
            },
            {
                test: /\.(png|svg|jpe?g|gif)$/,
                type: 'asset/resource',
            }
        ]
    }
}