#!/bin/bash

# Test fk join
python fkjoin.py \
    --table1_path wrapper_test_data/fk_departments.csv \
    --key1 'dept_id' \
    --payload1_cols 'dept_name' 'location' \
    --table2_path wrapper_test_data/fk_employees.csv \
    --key2 'department_id' \
    --payload2_cols 'emp_id' 'emp_name' \
    --output_path wrapper_test_data/fk_join_results.csv

# Test nfk join
python join.py \
    --table1_path wrapper_test_data/nfk_tags.csv \
    --key1 'tag_id' \
    --payload1_cols 'tag_name' 'category' \
    --table2_path wrapper_test_data/nfk_posts.csv \
    --key2 'tag_id' \
    --payload2_cols 'post_id' 'tagged_by_user' \
    --output_path wrapper_test_data/nfk_join_results.csv

# Test filter - filter for entries with score > 950
python operator1.py \
    --filepath wrapper_test_data/filter_test_data.csv \
    --output_path wrapper_test_data/filter_results.csv \
    --filter_col 'score' \
    --payload_cols 'user_id' 'username' 'country' \
    --filter_threshold_op1 950 \
    --filter_condition_op1 '<'

# Test aggregation
python operator2.py \
    --filepath wrapper_test_data/agg_data.csv \
    --output_path wrapper_test_data/agg_results.csv \
    --group_by_col 'region' \
    --agg_col 'revenue' \
    --payload_cols 'product_category' 'units_sold'
