function datasetGet(evt) {
    evt.preventDefault();
    var dataset_elem = document.getElementById('dataset-name-input');
    var dataset = dataset_elem.value;

    elem_visibility('data-table', 'none');
    elem_visibility('dataset-info-header', 'block');
    elem_visibility('dataset-profile-loader', 'block');

    var xhttp = new XMLHttpRequest();
    xhttp.open("POST", "/process_dataset_from_name", true);
    xhttp.setRequestHeader("Content-Type", "application/x-www-form-urlencoded");
    xhttp.send('data=' + JSON.stringify({dataset: dataset}));
    xhttp.onload = function() {
        var data = JSON.parse(xhttp.responseText);
        elem_visibility('dataset-profile-loader', 'none');
        populate_dataset_table(data);
        elem_visibility('dataset-save-div', 'block');
        elem_visibility('dataset-upload-dataset-table', 'block');
    }
}

function datasetSave(evt) {
    evt.preventDefault();
    var json_to_send = get_dataset_table_values();
    var xhttp = new XMLHttpRequest();
    xhttp.open("POST", "/dataset_db_save", true);
    xhttp.setRequestHeader("Content-Type", "application/x-www-form-urlencoded");
    xhttp.send('data=' + JSON.stringify(json_to_send));
    window.location.replace('/')
}



function check_if_same_dataset(dataset) {
    var elem = document.getElementById('dataset-table-row-col-1');
    if (elem.innerHTML == dataset) {
        return true
    }
    else {
        return false
    }
}

