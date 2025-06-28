#!/bin/bash

python operator3.py \
    --initial_filepath ~/obliviator/filter_test.csv \
    --filter_key_col_3_1 transaction_date \
    --id_col_3_1 record_id \
    --filter_threshold_3_1 19800101 \
    --operator3_variant default
