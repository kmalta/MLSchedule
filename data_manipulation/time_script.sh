#!bin/bash

python transform_time.py .5 True False synth > experiment_timings/communication_times
python transform_time.py .5 False False synth > experiment_timings/all_times_.5gb
python transform_time.py 1 False False synth > experiment_timings/all_times_1gb
python transform_time.py 2 False False synth > experiment_timings/all_times_2gb
python transform_time.py 4 False False synth > experiment_timings/all_times_4gb
python transform_time.py 8 False False synth > experiment_timings/all_times_8gb
python transform_time.py .5 False True synth > experiment_timings/computation_times_.5gb
python transform_time.py 1 False True synth > experiment_timings/computation_times_1gb
python transform_time.py 2 False True synth > experiment_timings/computation_times_2gb
python transform_time.py 4 False True synth > experiment_timings/computation_times_4gb
python transform_time.py 8 False True synth > experiment_timings/computation_times_8gb
