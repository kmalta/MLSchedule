var express = require('express');
var path = require('path');
var bodyParser = require('body-parser');
var app = express();


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

app.get('/', function (req, res, next) {

  res.redirect("/dataset_upload");
  //res.sendFile('./html/dataset_upload.html')
});

app.get('/dataset_upload', function (req, res, next) {
    res.render(path.resolve('src/views/dataset_upload.ejs'));
});

app.get('/profile', function (req, res, next) {
    res.render(path.resolve('src/views/profile.ejs'));
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
