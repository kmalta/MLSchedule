#!/bin/bash

# #SUSY and HIGGS
# python generate_fake_data.py 1 0 $1
# python generate_fake_data.py 1 -1 $1
# python generate_fake_data.py 2 0 $1
# python generate_fake_data.py 2 -1 $1

# #KDDB
# python generate_fake_data.py 7 -7 $1
# python generate_fake_data.py 8 -7 $1

# #URL
# python generate_fake_data.py 6 -4 $1
# python generate_fake_data.py 6 -5 $1
# python generate_fake_data.py 7 -4 $1

# #KDDA
# python generate_fake_data.py 7 -5 $1
# python generate_fake_data.py 7 -6 $1
# python generate_fake_data.py 8 -5 $1
# python generate_fake_data.py 8 -6 $1

#Communication Exps
python generate_fake_data.py 1 0 $1 $2 $3
python generate_fake_data.py 2 0 $1 $2 $3
python generate_fake_data.py 3 0 $1 $2 $3
python generate_fake_data.py 4 -1 $1 $2 $3
python generate_fake_data.py 5 -2 $1 $2 $3
python generate_fake_data.py 6 -3 $1 $2 $3
python generate_fake_data.py 7 -4 $1 $2 $3
python generate_fake_data.py 8 -5 $1 $2 $3
