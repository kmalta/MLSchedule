const ws = new WebSocket('ws://localhost:5000');

ws.onopen = function open() {
  ws.send(JSON.stringify({message: 'hello', client: 'dataset upload'}));
  // var dropdown = document.getElementById('profile-machine-count');
  // dropdown.value("4");
};

ws.onmessage = function incoming(event) {
    var message = JSON.parse(event.data);

    if (message['message'] == 'show loader') {
        if (check_if_same_dataset(message) == false) {
            elem_visibility('data-table', 'none');
            //elem_visibility('profile-budget-div', 'none');
            elem_visibility('dataset-info-header', 'block');
            elem_visibility('dataset-profile-loader', 'block');
        }
    }
    if (message['message'] == 'hide loader') {
        elem_visibility('dataset-profile-loader', 'none');
    }
    if (message['message'] == 'return profile data') {
        populate_profile_table(message);
        //change_profiling_default_amount(message);
        elem_visibility('dataset-save-div', 'block');
    }
    // if (message['message'] == 'budget change') {
    //     change_profiling_default_amount(message);
    // }

};


function elem_visibility(elemID, visibility) {
    var elem = document.getElementById(elemID);
    elem.style.display = visibility;
}

function populate_profile_table(message) {

    var values_to_populate = [message['name'],
                              message['url'],
                              message['size'],
                              message['samples'],
                              roundTo(parseFloat(message['features'], 3)).toString(),
                              message['inst type'],
                              '$' + parseFloat(message['bid']).toString()
                              ]

    for (i = 1; i < 8; i++) {
        var elem = document.getElementById('table-row-col-' + i.toString());
        elem.innerHTML = values_to_populate[i - 1];
    }

    elem_visibility('data-table', 'table');

    var table = document.getElementById('data-table');
    var table_str = table.innerHTML;
    ws.send(JSON.stringify({message: 'table', table: table_str}))

}


function change_profiling_default_amount(message) {
    console.log(message)
    var input = document.getElementById('profile-budget-input');
    input.value = '$' + roundTo(message['bid']*(message['num_workers'] + 1), 3).toString();
    elem_visibility('profile-budget-div', 'block');
}

function check_if_same_dataset(message) {
    var elem = document.getElementById('table-row-col-1');
    if (elem.innerHTML == message['url']) {
        return true
    }
    else {
        return false
    }
}


function roundTo(n, digits) {
    if (digits === undefined) {
        digits = 0;
    }

    var multiplicator = Math.pow(10, digits);
    n = parseFloat((n * multiplicator).toFixed(11));
    var test =(Math.round(n) / multiplicator);
    return +(test.toFixed(2));
}