import React, { Fragment } from 'react'
import ReactDOM from 'react-dom'
import DataState from "./context/data/dataState"
import Header from "./components/layout/Header"
import Data from "./components/pages/Data"
import Alert from './components/alerts/Alert'
export const App = () => {
    return (
        <DataState>
            <Header />
            <Alert />
            <div className="container-sm align-middle">
                <Data/>
            </div>
        </DataState>
    )
}

ReactDOM.render(<App />, document.getElementById("app"))
