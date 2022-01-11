import React, { useContext,useState } from 'react'
import DataContext from '../../context/data/dataContext'

const Form = ({data}) => {
    const dataContext = useContext(DataContext)
    const {getData} = dataContext
    const [state,setState] = useState({title:"",text:""})
    const onChange = (e) => {
        setState({...state,[e.target.name]:e.target.value});
    }

    const onSubmit = (e) => {
        e.preventDefault()
        getData(state)
        setState({title:"",text:""})
    }

    const  {title,text} = state;
    return (
        <div className="card card-body mt-4 mb-4">
            <h5 class="card-title text-center mb-1">
                {data.title}
            </h5>
            <table className="table table-responsive-sm">
                <tbody>
                    {Object.keys(data).map(key => (
                        <tr>
                            <td>{key}</td>
                            <td>{data[key]}</td>
                        </tr>
                    ))}
                </tbody>
            </table>
            <form onSubmit={onSubmit}>
                {Object.keys(state).map(key =>(
                    <div className="form-group">
                        <input onChange={onChange} className="form-control" 
                        name={key} placeholder={key} type={typeof(state[key]) === typeof("string") ? "text": "number"} value={state[key]} />
                    </div>
                ))}
                <div className="form-group">
                    <button type="submit" class="btn btn-primary form-control">Classify</button>
                </div>
            </form>
        </div>
    )
}

export default Form
