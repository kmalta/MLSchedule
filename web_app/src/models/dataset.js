var mongoose = require('mongoose');
var Schema = mongoose.Schema;

var datasetSchema = new Schema({
    name: String,
    s3url: String,
    size_in_bytes: Number,
    size: String,
    samples: Number,
    features: Number,
    machine_type: String,
    table: String
});

var Dataset = mongoose.model('Dataset', datasetSchema);
module.exports = Dataset;