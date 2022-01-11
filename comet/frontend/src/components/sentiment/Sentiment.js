import React from 'react'

const Sentiment = ({data}) => {
    let color = data.data != 0 ? "success" : "danger"
    return (
        <div className="container justify-content-center">
            <h3 className="text-center">
                <i className={`far fa-${data.data !=0 ? "smile-beam": "sad-cry"
                } text-${color} fa-7x`} />
            </h3>
            <h3 className={`text-center mt-3`}>
                {"data: "}
                <span className={`text-${color}`}>
                    {data.data == 0 ? "Fake": "Legit"}
                </span>
            </h3>
        </div>
    )
}

export default Sentiment
