import React, {useContext,useEffect,Fragment} from 'react';
import DataContext from "../../context/data/dataContext"
import Alert from "../alerts/Alert"
// import Sentiment from '../sentiment/Sentiment';
import Form from '../data/Form';

const Data = () => {
    const dataContext = useContext(DataContext)
    const {data,loading,title,text} = dataContext;
    useEffect(() => {

    },//eslint-disable-next-line
    [title]
    );
    return (
        <div className="card mt-4">
            <div className="card-body align-content-center justify-content-center">
                {loading || title.size < 1 ? (
                    <div className="container justify-content-center">
                        <h1 className="container justify-content-center">
                            {title[0].toUpperCase() + title.slice(1)}
                        </h1>
                        <h3 className="text-center">
                            <i className="fas fa-spinner text-primary fa-7x"></i>
                        </h3>
                        <h3 className="text-center mt-3">
                            {"data: ???"}
                        </h3>
                    </div>) : (
                        <Fragment>
                            <Alert />
                            <h1 className="card-title text-center mx-2">
                            {title[0].toUpperCase() + title.slice(1)}
                            </h1>
                            {/* <Sentiment data={data} /> */}
                            <Form data={data}/>
                        </Fragment>

                    )

                }
            </div>
        </div>
    );
};

export default Data
