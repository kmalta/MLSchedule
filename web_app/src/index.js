var express = require('express');
var path = require('path');
var bodyParser = require('body-parser');
var app = express();


app.set('port', (process.env.PORT || '3000'));
app.set('views', path.join(__dirname, './ejs'));
app.set('view engine', 'ejs');

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

var server = app.listen((process.env.PORT || '3000'), function () {
  console.log('Listening on port %d', server.address().port);
});

app.get('/', function (req, res, next) {
  res.redirect("/home");
});

app.get('/home', function (req, res, next) {
  res.end("we are home");
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
