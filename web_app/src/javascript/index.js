var express = require('express');
var path = require('path');
var bodyParser = require('body-parser');
var XMLHttpRequest = require("xmlhttprequest").XMLHttpRequest;
var app = express();

var mongoose = require('mongoose');
mongoUrl = 'mongodb://localhost:27017/mlwebapp';
mongoose.connect(mongoUrl);
var db = mongoose.connection;


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






//GETS
app.get('/', function (req, res) {
  res.render(path.resolve('src/views/datasets_display/datasets_display.ejs'));
});

app.get('/add_dataset', function (req, res) {
  res.render(path.resolve('src/views/dataset_upload/dataset_upload.ejs'));
});

app.get('/datasets_display', function (req, res) {
  res.render(path.resolve('src/views/datasets_display/datasets_display.ejs'));
});

app.get('/profile/:token', function(req, res) {
  res.render(path.resolve('src/views/profile/profile.ejs'));
});

app.get('/profile', function (req, res) {
  res.render(path.resolve('src/views/profile/profile.ejs'));
});

app.get('/back_button', function (req, res) {
  res.render(path.resolve('src/views/datasets_display/datasets_display.ejs'));
});


app.get('/get_dataset_db_entries', function(req, res) {
  Dataset.find({}, function (err, datasets) {
    res.send(datasets);
  });  
});








app.post('/process_dataset_from_name', function(req, res) {
  var xhttp = new XMLHttpRequest();
  xhttp.open("GET", "http://0.0.0.0:8080/process_dataset/" + JSON.parse(req.body.data).dataset, true);
  xhttp.send();
  xhttp.onload = function() {
    res.send(xhttp.responseText)
  }
});



app.post('/dataset_db_save', function(req, res){
  var dataset_json = JSON.parse(req.body.data);
  console.log(dataset_json);
  var dataset = new Dataset({
    name: dataset_json['name'],
    s3url: dataset_json['url'],
    size_in_bytes: dataset_json['size_in_bytes'],
    size: dataset_json['size'],
    samples: dataset_json['samples'],
    features: dataset_json['features'],
    machine_type: dataset_json['inst_type'],
    bid: dataset_json['bid']
  });
  console.log(dataset)
  dataset.save(function(err) {
    if (err) throw err;

    console.log('Dataset saved successfully!');
  });
  console.log('DB SAVED!')
});

app.post('/profile_button_submit', function(req, res) {
  var date = new Date();
  var profile = new Profile({
      time_submitted: date,
      dataset_id: req.body.datasetID,
      bid_per_machine: parseFloat(req.body.bidPerMachine),
      budget: parseFloat(req.body.budget.split("$")[1]),
      number_of_machines: parseInt(req.body.numberOfMachines),
      machine_type: req.body.machineType
  });
  profile.save(function(err) {
    if (err) throw err;

    console.log('Profile saved successfully!');
  });
  var xhttp = new XMLHttpRequest();
  var json_send = JSON.stringify(req.body);

  xhttp.open("GET", 'http://0.0.0.0:8080/submit_profile/' + json_send, true);
  xhttp.send();
  xhttp.onload = function() {
    console.log(xhttp.responseText)
  }

  res.redirect('/profile/' + profile._id);
});


app.post('/get_profile_db_entry', function(req, res) {
  var profile_id = JSON.parse(req.body.data).profile_id;
  Profile.find({_id: profile_id}, function (err, profile) {
    res.send(profile[0]);
  });  
});

app.post('/get_dataset_db_entry', function(req, res) {
  var dataset_id = JSON.parse(req.body.data).dataset_id;
  Dataset.find({_id: dataset_id}, function (err, datasets) {
    res.send(datasets[0]);
  });  
});


app.post('/get_dataset_profiles_db_entries', function(req, res) {
  var dataset_id = JSON.parse(req.body.data).dataset_id;
  Profile.find({dataset_id: dataset_id}, function (err, profiles) {
    console.log(profiles)
    res.send(profiles);
  });  
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
