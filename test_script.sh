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


