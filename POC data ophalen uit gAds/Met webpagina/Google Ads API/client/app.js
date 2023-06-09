SERVER_URL = "http://127.0.0.1:5000"

function handleLogin(response) {
    localStorage.setItem("token", response.credential);
    listAccessibleCustomers();
}

function onLinkAdsAccount() {
    token = localStorage.getItem("token");
    window.location.href = `${SERVER_URL}/authorize?token=${token}`;
}

function listAccessibleCustomers() {
    const url = `${SERVER_URL}/customers`;
    const xhr = new XMLHttpRequest();
    xhr.open("GET", url, true);
    xhr.setRequestHeader("Content-type", "application/json");
    xhr.setRequestHeader("token", localStorage.getItem("token"));
    xhr.send();
    xhr.onload = function() {
        if (xhr.status == 200){
            const response = JSON.parse(xhr.response);
            if ("name" in response && response.name == "INVALID_REFRESH_TOKEN"){
                onLinkAdsAccount();
                console.log(response);
            } else {
                console.log(response);
            }
        }
    }
}