#!/bin/bash -x


python join.py \
    --filepath1 ~/obliviator/data/person_kks_test.csv \
    --filepath2 ~/obliviator/data/comment_kks_test.csv \
    --join_key1 id \
    --id_col1 id \
    --join_key2 CreatorPersonId \
    --id_col2 comment_id;


python fkjoin.py \
    --filepath1 ~/obliviator/data/person_kks_test.csv \
    --id_col1 id \
    --join_key1 id \
    --filepath2 ~/obliviator/data/comment_kks_test.csv \
    --id_col2 comment_id \
    --join_key2 CreatorPersonId \
    --fk_join_variant default;


python operator1.py \
	--filepath ~/obliviator/data/op1_test_data.csv \
	--id_col my_id \
	--string_to_project_col my_string_data \
	--filter_threshold_op1 -1 \
	--operator1_variant default;


python operator2.py \
    --filepath ~/obliviator/data/op2_test_data.txt \
    --operator2_variant default;


python operator3.py \
    --initial_filepath ~/obliviator/data/my_filter_test_with_dates.csv \
    --filter_key_col_3_1 transaction_date \
    --id_col_3_1 record_id \
    --filter_threshold_3_1 19800101 \
    --join_key_col_3_2_A transaction_date \
    --join_key_col_3_2_B_and_values record_id,value \
    --second_table_filepath_3_2 ~/obliviator/data/my_join_second_table.csv \
    --second_table_key_col_3_2 tx_date \
    --second_table_other_cols_3_2 order_id,item,quantity \
    --col1_from_step2_output_3_3 t1_id \
    --col2_from_step2_output_3_3 t1_numeric \
    --col3_from_step2_output_3_3 t2_quantity \
    --operator3_variant default;
