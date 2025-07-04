#include "enclave/scalable_oblivious_join.h"
#include <limits.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>
#include <stdarg.h>
#include <threads.h>
#include <liboblivious/algorithms.h>
#include <liboblivious/primitives.h>
#include "common/elem_t.h"
#include "common/error.h"
#include "common/util.h"
#include "common/ocalls.h"
#include "common/code_conf.h"
#include "enclave/mpi_tls.h"
#include "enclave/parallel_enc.h"
#include "enclave/threading.h"
#include "enclave/oblivious_compact.h"

#ifndef DISTRIBUTED_SGX_SORT_HOSTONLY
#include <openenclave/enclave.h>
#include "enclave/parallel_t.h"
#endif



static int number_threads;
static bool *control_bit;

void reverse(char *s) {
    int i, j;
    char c;

    for (i = 0, j = strlen(s)-1; i<j; i++, j--) {
        c = s[i];
        s[i] = s[j];
        s[j] = c;
    }
}

int my_len(char *data) {
    int i = 0;

    while ((data[i] != '\0') && (i < DATA_LENGTH)) i++;
    
    return i;
}

void itoa(int n, char *s, int *len) {
    int i = 0;
    int sign;

    if ((sign = n) < 0) {
        n = -n;
        i = 0;
    }
        
    do {
        s[i++] = n % 10 + '0';
    } while ((n /= 10) > 0);
    
    if (sign < 0)
        s[i++] = '-';
    s[i] = '\0';
    
    *len = i;
    
    reverse(s);
}

int scalable_oblivious_join_init(int nthreads) {

    number_threads = nthreads;
    //aggregation_tree_init(number_threads);

    
    return 0;

}

struct args_op {
    int index_thread_start;
    int index_thread_end;
    elem_t* arr;
};

void aggregation_tree_op(void *voidargs) {
    struct args_op *args = (struct args_op*) voidargs;
    int index_thread_start = args->index_thread_start;
    int index_thread_end = args->index_thread_end;
    elem_t* arr = args->arr;

    for (int i = index_thread_start; i < index_thread_end; i++) {
        control_bit[i] = (19800101 <= arr[i].key);
    }

    return;
}

void scalable_oblivious_join(elem_t *arr, int length1, int length2, char* output_path){
    control_bit = calloc(length1, sizeof(*control_bit));
    int length_thread = length1 / number_threads;
    int length_extra = length1 % number_threads;
    (void)length2;
    int result_length = 0;
    struct args_op args_op_[number_threads];
    int idx_start_thread[number_threads + 1];
    idx_start_thread[0] = 0;
    struct thread_work multi_thread_aggregation_tree_1[number_threads - 1];
    init_time2();

    if (number_threads == 1) {
        for (int i = 0; i < length1; i++) {
            control_bit[i] = (19800101 <= arr[i].key);
        }
    } else {
        for (int i = 0; i < number_threads; i++) {
            idx_start_thread[i + 1] = idx_start_thread[i] + length_thread + (i < length_extra);

            args_op_[i].arr = arr;
            args_op_[i].index_thread_start = idx_start_thread[i];
            args_op_[i].index_thread_end = idx_start_thread[i + 1];
            if (i < number_threads - 1) {
                multi_thread_aggregation_tree_1[i].type = THREAD_WORK_SINGLE;
                multi_thread_aggregation_tree_1[i].single.func = aggregation_tree_op;
                multi_thread_aggregation_tree_1[i].single.arg = &args_op_[i];
                thread_work_push(&multi_thread_aggregation_tree_1[i]);
            }
        }
        aggregation_tree_op(&args_op_[number_threads - 1]);
        for (int i = 0; i < number_threads - 1; i++) {
            thread_wait(&multi_thread_aggregation_tree_1[i]);
        }
    }

    result_length = oblivious_compact_elem(arr, control_bit, length1, 1, number_threads);

    get_time2(true);

    char *char_current = output_path;
    for (int i = 0; i < result_length; i++) {
        int data_len1 = my_len(arr[i].data);
        
        strncpy(char_current, arr[i].data, data_len1);
        char_current += data_len1; char_current[0] = '\n'; char_current += 1;
    }
    char_current[0] = '\0';

    free(control_bit);
    return;
}
