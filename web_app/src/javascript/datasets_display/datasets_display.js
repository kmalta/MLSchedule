window.onload = grabDBEntryInfo();

function grabDBEntryInfo() {
    var xhttp = new XMLHttpRequest();
    xhttp.open("GET", "/get_dataset_db_entries", true);
    xhttp.setRequestHeader("Content-Type", "application/x-www-form-urlencoded");
    xhttp.send();
    xhttp.onload = function() {
        createDBTable(JSON.parse(xhttp.responseText));
    }
};

function createDBTable(data) {
    for (var i=0; i < data.length; i++) {
        populate_db_info_table(JSON.stringify(data[i]), i);
    }
    for (var i=0; i < data.length; i++) {
        var row = document.getElementById('profile-table-row-' + i.toString());
        row.onclick = (function(db_entry){
            return function(){
                populate_dataset_table(db_entry);

                elem_visibility('dataset-info-header', 'block');
                elem_visibility('data-table', 'table');
                elem_visibility('profile-budget-div', 'block');
                var bid = db_entry['bid'];

                var input = document.getElementById('profile-input-budget');
                input.value = '$' + roundTo(5*bid, 3).toString();

                var input = document.getElementById('profile-input-dataID');
                input.value = db_entry['_id']

                var input = document.getElementById('profile-input-bid');
                input.value = bid

                var input = document.getElementById('profile-input-machines');
                input.value = 5

                var input = document.getElementById('profile-input-machineType');
                input.value = db_entry['machine_type']

                var select = document.getElementById('profile-input-machine-count');
                select.value = 4
            }
        })(data[i]);
    }
};


function change_profiling_default_amount(num_workers) {
    var bid_elem = document.getElementById('dataset-table-row-col-7');
    var bid = parseFloat(bid_elem.innerHTML.split("$")[1]);
    var input = document.getElementById('profile-input-budget');
    input.value = '$' + roundTo(bid*(parseInt(num_workers) + 1), 3).toString();

    var input = document.getElementById('profile-input-machines');
    input.value = parseInt(num_workers)
    elem_visibility('profile-budget-div', 'block');
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

};

