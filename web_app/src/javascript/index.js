var express = require('express');
var path = require('path');
var bodyParser = require('body-parser');
var app = express();

var mongoose = require('mongoose');
mongoUrl = 'mongodb://localhost:27017/mlwebapp';
mongoose.connect(mongoUrl);
var db = mongoose.connection;

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
app.use('/models',express.static(path.resolve('src/models')));


var Dataset = require(path.resolve('src/models/dataset.js'));
var Profile = require(path.resolve('src/models/profile.js'));


var server = app.listen((process.env.PORT || '3000'), function () {
  console.log('Listening on port %d', server.address().port);
});



db.on('error', console.error.bind(console, 'connection error:'));


const wss = new WebSocket.Server({ server });

profile_table_str = null;
budget_amount = null;
dataset_info_json = null;

python_ws = null;
dataset_upload_ws = null;
dataset_profile_table_ws = null;
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

      if (message['client'] == 'dataset profile table') {
        dataset_profile_table_ws = _ws;
        console.log('Hello Dataset Profile Table Client JS :)')
        Dataset.find({name: 'susy'}, function (err, datasets) {
          dataset_profile_table_ws.send(JSON.stringify({message: 'db table', db_json: datasets}))
       });
      }

      if (message['client'] == 'profile') {
        profile_ws = _ws;
        console.log('Hello Profile Client JS :)')
        profile_ws.send(JSON.stringify({message: 'table', table: profile_table_str, budget: budget_amount, dataset_info: dataset_info_json}));
      }

    }

    if (message['message'] == 'return profile data') {
      dataset_info_json = JSON.stringify(message)
      dataset_upload_ws.send(JSON.stringify({message: 'hide loader'}));
      dataset_upload_ws.send(JSON.stringify(message));
    }
    if (message['message'] == 'table') {
      profile_table_str = message['table'];
    }

    if (message['message'] == 'budget change') {
      number_of_workers = parseInt(message['num_workers']);
      new_message = JSON.parse(dataset_info_json);
      new_message['num_workers'] = number_of_workers;
      budget_amount = (number_of_workers + 1)*new_message['bid'];
      dataset_info_json = JSON.stringify(new_message)
      dataset_upload_ws.send(JSON.stringify(new_message));
    }

  });
});


app.get('/', function (req, res, next) {
  res.render(path.resolve('src/views/dataset_profile_table_page.ejs'));
});



// Dataset upload.
app.get('/dataset_upload', function (req, res, next) {
  res.render(path.resolve('src/views/dataset_upload.ejs'));
});


// Dataset upload.
app.get('/dataset_profile_table_page', function (req, res, next) {
  res.render(path.resolve('src/views/dataset_profile_table_page.ejs'));
});

// Post for s3 url obtained through a form.
app.post('/dataset_upload', function(req, res){

  python_ws.send(JSON.stringify({message: 'profile data', dataset: req.body.dataset}));
  dataset_upload_ws.send(JSON.stringify({message: 'show loader'}));

});


app.post('/dataset_db_save', function(req, res){
  var dataset_json = JSON.parse(dataset_info_json);
  console.log(dataset_json)
  var dataset = new Dataset({
    name: dataset_json['name'],
    s3url: dataset_json['url'],
    size_in_bytes: dataset_json['size_in_bytes'],
    size: dataset_json['size'],
    samples: dataset_json['samples'],
    features: dataset_json['features'],
    machine_type: dataset_json['inst_type'],
    table: profile_table_str
  });
  dataset.save(function(err) {
    if (err) throw err;

    console.log('Dataset saved successfully!');
  });
  console.log('DB SAVED!')

  res.redirect('/dataset_profile_table_page');
});

app.post('/budget_update', function(req, res) {
  budget_amount = req.body.budget
  //refresh table
});

app.get('/add_dataset', function(req, res) {
  res.render(path.resolve('src/views/dataset_upload.ejs'));
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
