const ws = new WebSocket('ws://localhost:5000');

ws.onopen = function open() {
  ws.send(JSON.stringify({message: 'hello', client: 'dataset profile table'}));
};

ws.onmessage = function incoming(event) {
    var message = JSON.parse(event.data);
    if (message['message'] == 'db table') {
        for (var i=0; i < message['db_json'].length; i++) {
            populate_db_info_table(JSON.stringify(message['db_json'][i]), i);
        }
        for (var i=0; i < message['db_json'].length; i++) {
            var row = document.getElementById('profile-table-row-' + i.toString());
            row.onclick = (function(table){
                return function(){
                    table_row = document.getElementById('profile-data-table');
                    table_row.innerHTML = table;
                    elem_visibility('profile-dataset-info-header', 'block');
                    elem_visibility('profile-data-table', 'table');
                    elem_visibility('profile-budget-div', 'table');
                }
            })(message['db_json'][i]['table']);
        }
    }
};




function populate_db_info_table(db_entry, idx) {
    db_entry = JSON.parse(db_entry)
    var values_to_populate = [db_entry['name'],
                              db_entry['_id'],
                              db_entry['s3url'],
                              db_entry['size'],
                              db_entry['samples'],
                              db_entry['features'],
                              ]

    var elem = document.getElementById('db-data-table');
    var table = elem.innerHTML;
    var row_id = 'profile-table-row-' + idx.toString();


    var html_to_add = "<tr id='" + row_id + "'>";

    for (i = 1; i < values_to_populate.length + 1; i++) {
        var entry_id = 'profile-table-row-col-' + idx.toString() + '-' + i.toString();
        html_to_add = html_to_add + "<td id='" + entry_id + "'>" + values_to_populate[i - 1] + "</td>"
    }
    html_to_add = html_to_add + "</tr>"
    elem.innerHTML = table + html_to_add

}



function createClickHandler(table) {
    return function (event) {
        console.log(event)

    };
};



function elem_visibility(elemID, visibility) {
    var elem = document.getElementById(elemID);
    elem.style.display = visibility;
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