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




function roundTo(n, digits) {
    if (digits === undefined) {
        digits = 0;
    }

    var multiplicator = Math.pow(10, digits);
    n = parseFloat((n * multiplicator).toFixed(11));
    var test =(Math.round(n) / multiplicator);
    return +(test.toFixed(2));
}