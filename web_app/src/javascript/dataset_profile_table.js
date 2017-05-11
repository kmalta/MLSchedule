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
                table = populate_profile_table(db_entry);
                table_row = document.getElementById('profile-data-table');
                table_row.innerHTML = table;
                elem_visibility('profile-dataset-info-header', 'block');
                elem_visibility('profile-data-table', 'table');
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


function populate_profile_table(message) {
    console.log(message);
    var values_to_populate = [message['name'],
                              message['s3url'],
                              humanFileSize(parseInt(message['size_in_bytes']), true),
                              message['samples'],
                              roundTo(parseFloat(message['features'], 3)).toString(),
                              message['machine_type'],
                              '$' + parseFloat(message['bid']).toString()
                              ]

    for (i = 1; i < 8; i++) {
        var elem = document.getElementById('dataset-table-row-col-' + i.toString());
        elem.innerHTML = values_to_populate[i - 1];
    }

    elem_visibility('data-table', 'table');
};

function elem_visibility(elemID, visibility) {
    var elem = document.getElementById(elemID);
    elem.style.display = visibility;
};

function roundTo(n, digits) {
    if (digits === undefined) {
        digits = 0;
    }

    var multiplicator = Math.pow(10, digits);
    n = parseFloat((n * multiplicator).toFixed(11));
    var test =(Math.round(n) / multiplicator);
    return +(test.toFixed(2));
};


function humanFileSize(bytes, si) {
    var thresh = si ? 1000 : 1024;
    if(Math.abs(bytes) < thresh) {
        return bytes + ' B';
    }
    var units = si
        ? ['kB','MB','GB','TB','PB','EB','ZB','YB']
        : ['KiB','MiB','GiB','TiB','PiB','EiB','ZiB','YiB'];
    var u = -1;
    do {
        bytes /= thresh;
        ++u;
    } while(Math.abs(bytes) >= thresh && u < units.length - 1);
    return bytes.toFixed(1)+' '+units[u];
};

function unhumanize(text) { 
    var powers = {'k': 1, 'm': 2, 'g': 3, 't': 4};
    var regex = /(\d+(?:\.\d+)?)\s?(k|m|g|t)?b?/i;

    var res = regex.exec(text);

    return parseInt(res[1] * Math.pow(1000, powers[res[2].toLowerCase()]));
};
