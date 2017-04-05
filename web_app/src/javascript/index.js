var express = require('express');
var path = require('path');
var bodyParser = require('body-parser');
var app = express();
// var fs = require('fs');
// var vm = require('vm');

const WebSocket = require('ws');





app.set('port', (process.env.PORT || '3000'));
app.set('views', path.resolve('src/views'));
app.set('view engine', 'ejs');

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));
app.use('/css',express.static(path.resolve('src/css')));
app.use('/views',express.static(path.resolve('src/views')));
app.use('/data',express.static(path.resolve('src/data_files')));
app.use('/javascript',express.static(path.resolve('src/javascript')));


var server = app.listen((process.env.PORT || '3000'), function () {
  console.log('Listening on port %d', server.address().port);
});



 
const wss = new WebSocket.Server({ server });

profile_table_str = null;
budget_amount = null;
dataset_info_json = null;

python_ws = null;
dataset_upload_ws = null;
profile_ws = null;

wss.on('connection', function(_ws) {
  console.log("Connected to Python client");
  _ws.on('message', function incoming(data) {

    message = JSON.parse(data);

    if (message['message'] == 'hello') {
      if (message['client'] == 'python') {
        python_ws = _ws;
        console.log('Hello Python :)')
      }
      if (message['client'] == 'dataset upload') {
        dataset_upload_ws = _ws;
        console.log('Hello Dataset Upload Client JS :)')
      }

      if (message['client'] == 'profile') {
        profile_ws = _ws;
        console.log('Hello Profile Client JS :)')
        profile_ws.send(JSON.stringify({message: 'table', table: profile_table_str, budget: budget_amount, dataset_info: dataset_info_json}));
      }

    }

    if (message['message'] == 'return profile data') {
      dataset_info_json = data;
      dataset_upload_ws.send(JSON.stringify({message: 'hide loader'}));
      dataset_upload_ws.send(JSON.stringify(message));
    }
    if (message['message'] == 'table') {
      profile_table_str = message['table']
    }

  });
});


app.get('/', function (req, res, next) {

  res.redirect("/dataset_upload");
});



// Dataset upload.
app.get('/dataset_upload', function (req, res, next) {
    res.render(path.resolve('src/views/dataset_upload.ejs'));
});

// Post for s3 url obtained through a form.
app.post('/dataset_upload', function(req, res){
  if (req.body.url != null) {
    python_ws.send(JSON.stringify({message: 'profile data', url: req.body.url}));
    dataset_upload_ws.send(JSON.stringify({message: 'show loader'}));
  }
  if (req.body.budget != null) {
    budget_amount = req.body.budget
    res.redirect("/profile");
  }
});





app.get('/profile', function (req, res, next) {
  if (profile_table_str == null) {
    res.redirect("/dataset_upload");
  }
  else {
    res.render(path.resolve('src/views/profile.ejs'));
    profile_ws.send(JSON.stringify({message: 'table', table: profile_table_str}));
  }
});



// catch 404 and forward to error handler
app.use(function (req, res, next) {
  var err = new Error('Not Found');
  err.status = 404;
  next(err);
});


// production error handler
app.use(function (err, req, res, next) {
  res.status(err.status || 500);
  next(err);
});



module.exports = app;
