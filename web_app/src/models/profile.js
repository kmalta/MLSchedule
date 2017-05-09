var mongoose = require('mongoose');
var Schema = mongoose.Schema;

var profileSchema = new Schema({
    dataset_id: Number,
    bid_per_machine: Number,
    budget: Number,
    cost: Number,
    number_of_machines: Number,
    machine_type: String,
    comm_profile: [Number],
    full_profile: [Number],
    associated_synth_comm_profile: [Number]
});

var Profile = mongoose.model('Profile', profileSchema);

module.exports = Profile;