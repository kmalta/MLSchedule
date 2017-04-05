const ws = new WebSocket('ws://localhost:5000');

ws.onopen = function open() {
  ws.send(JSON.stringify({message: 'hello', client: 'profile'}));
};

ws.onmessage = function incoming(event) {
    var message = JSON.parse(event.data);

    if (message['message'] == 'table') {
      window.onload = function() {
        table_row = document.getElementById('profile-data-table');
        table_row.innerHTML = message['table'];
        elem_visibility('profile-dataset-info-header', 'block');
        populate_profile_info_table(message);
      }
    }

};


function elem_visibility(elemID, visibility) {
    var elem = document.getElementById(elemID);
    elem.style.display = visibility;
}



function populate_profile_info_table(message) {

    console.log(message)

    dataset_info_json = JSON.parse(message['dataset_info'])
    var values_to_populate = [message['budget'],
                              dataset_info_json['inst type'],
                              8,
                              11,
                              '$' + parseFloat(dataset_info_json['bid']).toString()
                              ]

    for (i = 1; i < values_to_populate.length + 1; i++) {
        var elem = document.getElementById('profile-table-row-col-' + i.toString());
        elem.innerHTML = values_to_populate[i - 1];
    }

    elem_visibility('profile-data-table', 'table');

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