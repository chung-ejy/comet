import { GET_DATA, SET_TITLE, SET_TEXT, SET_LOADING, STOP_LOADING, SET_ERROR, CLEAR_ERROR } from "./types";
export default(state,action) => {
    switch(action.type) {
        case SET_ERROR:
            return {
                ...state,
                error: {msg:"rekt",type:"big sadge"}
            }
        case CLEAR_ERROR:
            return {
                ...state,
                error:null
            }
        case SET_LOADING:
            return {
                ...state,
                loading:true
            }
        case SET_TEXT:
            return {
                ...state,
                text: action.payload
            }
        case SET_TITLE:
            return {
                ...state,
                title:action.payload
            }
        case GET_DATA:
            return {
                ...state,
                data:action.payload,
                loading:false
            }
    }
}