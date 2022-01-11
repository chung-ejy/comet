import React, { useContext } from "react";
import DataContext from "../../context/data/dataContext"

const Alert = () => {
    const dataContext = useContext(DataContext)
    const { error } = dataContext
    return (
        error !== null && (
            <div className="alert alert-danger mt-2" role="alert">
                {error.msg}
            </div>
        )
    )
}

export default Alert
