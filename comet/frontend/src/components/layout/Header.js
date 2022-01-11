import React, { Component } from 'react'

export class Header extends Component {
    render() {
        return (
            <nav class="navbar navbar-expand-lg item-center navbar-dark bg-primary">
                <div class="container-fluid">
                    <a class="navbar-brand mx-auto" href="#">Comet</a>
                </div>
            </nav>
        )
    }
}

export default Header
