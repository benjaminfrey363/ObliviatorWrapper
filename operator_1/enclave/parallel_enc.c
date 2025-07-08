#include <stdio.h>
#include <liboblivious/primitives.h>
#include "common/defs.h"
#include "common/elem_t.h"
#include "common/error.h"
#include "common/algorithm_type.h"
#include "common/util.h"
#include "enclave/crypto.h"
#include "enclave/mpi_tls.h"
#include "enclave/threading.h"
#include "enclave/scalable_oblivious_join.h"

#ifndef DISTRIBUTED_SGX_SORT_HOSTONLY
#include <openenclave/enclave.h>
#include "enclave/parallel_t.h"
#endif

int world_rank;
int world_size;

static elem_t *arr;

void ecall_release_threads(void) {
    thread_release_all();
}

void ecall_unrelease_threads(void) {
    thread_unrelease_all();
}

int ecall_ojoin_init(int world_rank_, int world_size_, size_t num_threads) {
    int ret;

    world_rank = world_rank_;
    world_size = world_size_;
    total_num_threads = num_threads;

    ret = rand_init();
    if (ret) {
        handle_error_string("Error initializing RNG");
        goto exit;
    }

    /*
    ret = mpi_tls_init(world_rank, world_size, &entropy_ctx);
    if (ret) {
        handle_error_string("Error initializing MPI-over-TLS");
        goto exit_free_rand;
    }
    */

    scalable_oblivious_join_init(num_threads);

exit:
    return ret;

// exit_free_rand:
    rand_free();
    return ret;
}

void ecall_ojoin_free_arr(void) {
    // mpi_tls_bytes_sent = 0;
}

void ecall_ojoin_free(void) {
    // mpi_tls_free();
    rand_free();
}

void ecall_start_work(void) {
    thread_start_work();
}

// default obliviator function
/*
int ecall_scalable_oblivious_join(char *input_path, size_t len) {
    (void)len;

    char *length;
    length = strtok(input_path, "\n");
    int length1 = atoi(length);
    int length2 = 0;
    arr = calloc((length1 + length2), sizeof(*arr));
    for (int i = 0; i < length1; i++) {
        arr[i].key = atoi(strtok(NULL, " "));
        strncpy(arr[i].data, strtok(NULL, "\n"), DATA_LENGTH);
    }

    scalable_oblivious_join(arr, length1, length2, input_path);
    
    free(arr);

    return 0;
}
*/



// Modified to parse 64-bit keys
//int ecall_scalable_oblivious_join(char *input_path, size_t len) {
    //(void)len;

    /*
    char *length;
    length = strtok(input_path, "\n");
    int length1 = atoi(length);
    int length2 = 0;
    arr = calloc((length1 + length2), sizeof(*arr));
    
    // The corrected loop
    for (int i = 0; i < length1; i++) {
        // Use atoll for 64-bit keys
        arr[i].key = atoll(strtok(NULL, " ")); 
        strncpy(arr[i].data, strtok(NULL, "\n"), DATA_LENGTH);
    }
    */

    // Changed to:
    // Corrected parsing logic for enclave/parallel_enc.c
/*
    char *line_iterator;
    char *line;

    // Get the header line
    line = strtok_r(input_path, "\n", &line_iterator);
    int length1 = atoi(line);
    int length2 = 0; // Assuming length2 is not in the header for this operator

    arr = calloc((length1 + length2), sizeof(*arr));

    // Loop through the data lines
    for (int i = 0; i < length1; i++) {
        // Get the next full line
        line = strtok_r(NULL, "\n", &line_iterator);
        if (line == NULL) {
            break; // Stop if we run out of lines
        }

        char *token_iterator;
        char *key_str;
        char *val_str;

        // Get the key from the current line
        key_str = strtok_r(line, " ", &token_iterator);
        // Get the rest of the line as the value
        val_str = strtok_r(NULL, "\n", &token_iterator);

        if (key_str) {
            arr[i].key = atoll(key_str);
        }
        if (val_str) {
            strncpy(arr[i].data, val_str, DATA_LENGTH);
        }
    }

    scalable_oblivious_join(arr, length1, length2, input_path);
    
    free(arr);

    return 0;
}
*/

// FINAL FIX: modified to have separate input and output buffers
int ecall_scalable_oblivious_join(char *input_path, size_t len) {
    (void)len;

    // --- Parsing Logic (this part is correct and stays the same) ---
    char *line_iterator;
    char *line;
    line = strtok_r(input_path, "\n", &line_iterator);
    int length1 = atoi(line);
    int length2 = 0;
    arr = calloc((length1 + length2), sizeof(*arr));
    for (int i = 0; i < length1; i++) {
        line = strtok_r(NULL, "\n", &line_iterator);
        if (line == NULL) { break; }
        char *token_iterator;
        char *key_str = strtok_r(line, " ", &token_iterator);
        char *val_str = strtok_r(NULL, "\n", &token_iterator);
        if (key_str) { arr[i].key = atoll(key_str); }
        //if (val_str) { strncpy(arr[i].data, val_str, DATA_LENGTH); }
        // change value string handling to be safe - always null-terminated
        // Corrected to respect 27 byte buffer size
        if (val_str) {
            // Copy up to 26 characters into the 27-byte buffer
            strncpy(arr[i].data, val_str, 26);
            // Place the null terminator in the last position
            arr[i].data[26] = '\0';
        }
    }

    // --- FIX: Create a separate output buffer ---
    char *output_buffer = (char *)malloc(len);
    if (output_buffer == NULL) {
        free(arr);
        return -1; // Indicate memory allocation error
    }
    output_buffer[0] = '\0'; // Start with an empty string

    // --- Processing Step ---
    // Call the function, but pass the NEW buffer as the output destination.
    scalable_oblivious_join(arr, length1, length2, output_buffer);
    
    // --- Finalization ---
    // Copy the results from our clean output buffer back to the original buffer
    // that the host can see.
    strncpy(input_path, output_buffer, len);
    if (len > 0) {
        input_path[len - 1] = '\0'; // Ensure null termination
    }
    
    // Clean up all allocated memory
    free(output_buffer);
    free(arr);

    return 0;
}

