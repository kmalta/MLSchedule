function populate_dataset_table(message) {

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
}

function get_dataset_table_values() {
    var arr = ['name', 'url', 'size', 'samples', 'features', 'inst_type', 'bid'];
    var dict = {};
    for (i = 1; i < 8; i++) {
        var elem = document.getElementById('dataset-table-row-col-' + i.toString());
        dict[arr[i-1]] = elem.innerHTML;
    }
    dict['size_in_bytes'] = unhumanize(dict['size']);
    dict['bid'] = parseFloat(dict['bid'].split("$")[1]);
    return dict;
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

function elem_visibility(elemID, visibility) {
    var elem = document.getElementById(elemID);
    elem.style.display = visibility;
}


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
}

function unhumanize(text) { 
    var powers = {'k': 1, 'm': 2, 'g': 3, 't': 4};
    var regex = /(\d+(?:\.\d+)?)\s?(k|m|g|t)?b?/i;

    var res = regex.exec(text);

    return parseInt(res[1] * Math.pow(1000, powers[res[2].toLowerCase()]));
}