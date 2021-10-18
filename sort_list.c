//
// Sorts a list using multiple threads
//

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <time.h>
#include <limits.h>

#define MAX_THREADS     65536
#define MAX_LIST_SIZE   100000000

#define DEBUG 0

// Thread variables
//
// VS: ... declare thread variables, mutexes, condition varables, etc.,
// VS: ... as needed for this assignment 
//

pthread_barrier_t start_barrier;
pthread_barrier_t stop_barrier;
// Global variables
int num_threads;		// Number of threads to create - user input 
int list_size;			// List size
int *list;			// List of values
int *work;			// Work array
int *list_orig;			// Original list of values, used for error checking

// Print list - for debugging
void print_list(int *list, int list_size) {
    int i;
    for (i = 0; i < list_size; i++) {
        printf("[%d] \t %16d\n", i, list[i]); 
    }
    printf("--------------------------------------------------------------------\n"); 
}

// Comparison routine for qsort (stdlib.h) which is used to 
// a thread's sub-list at the start of the algorithm
int compare_int(const void *a0, const void *b0) {
    int a = *(int *)a0;
    int b = *(int *)b0;
    if (a < b) {
        return -1;
    } else if (a > b) {
        return 1;
    } else {
        return 0;
    }
}

// Return index of first element larger than or equal to v in sorted list
// ... return last if all elements are smaller than v
// ... elements in list[first], list[first+1], ... list[last-1]
//
//   int idx = first; while ((v > list[idx]) && (idx < last)) idx++;
//
int binary_search_lt(int v, int *list, int first, int last) 
{
    // Linear search code
    // int idx = first; while ((v > list[idx]) && (idx < last)) idx++; return idx;

    int left = first; 
    int right = last-1; 

    if (list[left] >= v) return left;
    if (list[right] < v) return right+1;
    int mid = (left+right)/2; 
    while (mid > left) {
        if (list[mid] < v) {
	    left = mid; 
	} else {
	    right = mid;
	}
	mid = (left+right)/2;
    }
    return right;
}
// Return index of first element larger than v in sorted list
// ... return last if all elements are smaller than or equal to v
// ... elements in list[first], list[first+1], ... list[last-1]
//
//   int idx = first; while ((v >= list[idx]) && (idx < last)) idx++;
//
int binary_search_le(int v, int *list, int first, int last) {

    // Linear search code
    // int idx = first; while ((v >= list[idx]) && (idx < last)) idx++; return idx;
 
    int left = first; 
    int right = last-1; 

    if (list[left] > v) return left; 
    if (list[right] <= v) return right+1;
    int mid = (left+right)/2; 
    while (mid > left) {
        if (list[mid] <= v) {
	    left = mid; 
	} else {
	    right = mid;
	}
	mid = (left+right)/2;
    }
    return right;
}

// Sort list via parallel merge sort
//
// VS: ... to be parallelized using threads ...
//


struct thread_instance 
{
    int id;
    int chunk;
    int level;
    int *start_ind;
};

void *sorter(void *arguments)
{
    struct thread_instance *thread_meta = arguments;
    int id = (*thread_meta).id;
    int chunk = (*thread_meta).chunk;
    int *start_ind = (int *)(*thread_meta).start_ind;
    int level = (*thread_meta).level;
    int my_own_blk, my_own_idx, my_list_size;
    int my_blk_size, my_search_blk, my_search_idx, my_search_idx_max;
    int my_write_blk, my_write_idx;
    int my_search_count; 
    int idx, i_write;

    int current_level=-1;
    int i;


    while(current_level<level)
    {
        if(current_level==-1)
        {
            pthread_barrier_wait(&start_barrier);
            my_list_size = *(start_ind+id+1)-*(start_ind+id);
            qsort(&list[*(start_ind+id)], my_list_size, sizeof(int), compare_int);
            //printf("output of qsort on id %d\n", id);
            //for(i=*(start_ind+id);i<*(start_ind+id+1);i++)
            //{
            //    printf("%d, ",list[i]);
            //}
            //printf("\n");
            pthread_barrier_wait(&stop_barrier); 
        }
        else
        {
            pthread_barrier_wait(&start_barrier);
            my_blk_size = chunk * (1 << current_level); //block size
            my_own_blk = ((id >> current_level) << current_level); 
            my_own_idx = *(start_ind+my_own_blk);
            my_search_blk = ((id >> current_level) << current_level) ^ (1 << current_level);
            my_search_idx = *(start_ind+my_search_blk);
            my_search_idx_max = my_search_idx+my_blk_size;

            my_write_blk = ((id >> (current_level+1)) << (current_level+1));
            my_write_idx = *(start_ind+my_write_blk);

            idx = my_search_idx;
            //printf("Current level is %d and my_id is %d  my_search_starts at %d ends at %d  my_write_blk is %d my_write_idx is %d\n",current_level,id,my_search_idx, my_search_idx_max, my_write_blk, my_write_idx);
            my_search_count = 0;

            if (my_search_blk > my_own_blk) 
            {
                idx = binary_search_lt(list[*(start_ind+id)], list, my_search_idx, my_search_idx_max); 
            } 
            else 
            {
                idx = binary_search_le(list[*(start_ind+id)], list, my_search_idx, my_search_idx_max); 
            }
            my_search_count = idx - my_search_idx;
            i_write = my_write_idx + my_search_count + (*(start_ind+id)-my_own_idx); 
            work[i_write] = list[*(start_ind+id)];
            // printf("Binary: Just wrote %d at %d on id %d at level %d\n", work[i_write], i_write, id, current_level);



            // Linear search for 2nd element onwards
            for (i = *(start_ind+id)+1; i < *(start_ind+id+1); i++) 
            {
                if (my_search_blk > my_own_blk) 
                {
                    while ((list[i] > list[idx]) && (idx < my_search_idx_max)) 
                    {
                        idx++; my_search_count++;
                    }
                } 
                else 
                {
                    while ((list[i] >= list[idx]) && (idx < my_search_idx_max)) 
                    {
                        idx++; my_search_count++;
                    }
                }
                i_write = my_write_idx + my_search_count + (i-my_own_idx); 
                work[i_write] = list[i];
                // printf("Linear: Just wrote %d at %d on id %d at level %d\n", work[i_write], i_write, id, current_level);
            }
            pthread_barrier_wait(&stop_barrier);
            for (i = *(start_ind+id); i < *(start_ind+id+1); i++) 
            {
                // printf("On thread id = %d writing value %d on %d at level %d\n", id, work[i], i, current_level);
	            list[i] = work[i];
	        }
        } 
        current_level++;
    }
        
}


// Main program - set up list of random integers and use threads to sort the list
//
// Input: 
//	k = log_2(list size), therefore list_size = 2^k
//	q = log_2(num_threads), therefore num_threads = 2^q
//
int main(int argc, char *argv[]) {

    struct timespec start, stop, stop_qsort;
    double total_time, time_res, total_time_qsort;
    int k, q, j, error; 
    int my_id,i;

    // Read input, validate
    if (argc != 3) {
	printf("Need two integers as input \n"); 
	printf("Use: <executable_name> <log_2(list_size)> <log_2(num_threads)>\n"); 
	exit(0);
    }
    k = atoi(argv[argc-2]);
    if ((list_size = (1 << k)) > MAX_LIST_SIZE) {
	printf("Maximum list size allowed: %d.\n", MAX_LIST_SIZE);
	exit(0);
    }; 
    q = atoi(argv[argc-1]);
    if ((num_threads = (1 << q)) > MAX_THREADS) {
	printf("Maximum number of threads allowed: %d.\n", MAX_THREADS);
	exit(0);
    }; 
    if (num_threads > list_size) {
	printf("Number of threads (%d) < list_size (%d) not allowed.\n", 
	   num_threads, list_size);
	exit(0);
    }; 

    // Allocate list, list_orig, and work

    list = (int *) malloc(list_size * sizeof(int));
    list_orig = (int *) malloc(list_size * sizeof(int));
    work = (int *) malloc(list_size * sizeof(int));

    int ptr[num_threads+1];
    int np = list_size/num_threads; 
    for (my_id = 0; my_id < num_threads; my_id++) {
        ptr[my_id] = my_id * np;
    }
    ptr[num_threads] = list_size;

//
// VS: ... May need to initialize mutexes, condition variables, 
// VS: ... and their attributes
//

    pthread_barrier_init(&start_barrier, NULL, num_threads);
    pthread_barrier_init(&stop_barrier, NULL, num_threads);

    pthread_t p_threads[num_threads];
    pthread_attr_t attr;

    // Initialize list of random integers; list will be sorted by 
    // multi-threaded parallel merge sort
    // Copy list to list_orig; list_orig will be sorted by qsort and used
    // to check correctness of multi-threaded parallel merge sort
    srand48(0); 	// seed the random number generator
    for (j = 0; j < list_size; j++) {
	list[j] = (int) lrand48();
	list_orig[j] = list[j];
    }
    // duplicate first value at last location to test for repeated values
    list[list_size-1] = list[0]; list_orig[list_size-1] = list_orig[0];

    // Create threads; each thread executes find_minimum
    clock_gettime(CLOCK_REALTIME, &start);

//
// VS: ... may need to initialize mutexes, condition variables, and their attributes
//

// Serial merge sort 
// VS: ... replace this call with multi-threaded parallel routine for merge sort
// VS: ... need to create threads and execute thread routine that implements 
// VS: ... parallel merge sort
    
    for (i=0;i<num_threads;i++)
    {
        struct thread_instance *args = malloc (sizeof (struct thread_instance));
        (*args).id = i;
        (*args).chunk = np;
        (*args).start_ind = ptr;
        (*args).level = q;
        pthread_create(&p_threads[i], NULL, &sorter, (void *) args);
    }

    for (i=0;i<num_threads;i++)
    {
        pthread_join(p_threads[i], NULL);
    }


    // Compute time taken
    clock_gettime(CLOCK_REALTIME, &stop);
    total_time = (stop.tv_sec-start.tv_sec)
	+0.000000001*(stop.tv_nsec-start.tv_nsec);

    // Check answer
    qsort(list_orig, list_size, sizeof(int), compare_int);
    clock_gettime(CLOCK_REALTIME, &stop_qsort);
    total_time_qsort = (stop_qsort.tv_sec-stop.tv_sec)
	+0.000000001*(stop_qsort.tv_nsec-stop.tv_nsec);

    error = 0; 
    for (j = 1; j < list_size; j++) {
        //printf("Your val = %d expected %d\n",list[j],list_orig[j] );
	if (list[j] != list_orig[j]) error = 1; 
    }

    if (error != 0) {
	printf("Houston, we have a problem!\n"); 
    }

    // Print time taken
    printf("List Size = %d, Threads = %d, error = %d, time (sec) = %8.4f, qsort_time = %8.4f\n", 
	    list_size, num_threads, error, total_time, total_time_qsort);

// VS: ... destroy mutex, condition variables, etc.
    pthread_barrier_destroy(&start_barrier);
    pthread_barrier_destroy(&stop_barrier);
    free(list); free(work); free(list_orig); 
    return 0;
}

