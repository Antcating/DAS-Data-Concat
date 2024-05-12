import numpy as np
from concurrent.futures import ThreadPoolExecutor


def multithreaded_sum(arr, num_threads):
    # Define a function to sum each chunk
    def sum_chunk(chunk):
        return np.sum(chunk, axis=-1, dtype=np.float32)

    # # Preallocate the result array
    # result = np.zeros(arr.shape[0], arr.shape[1])
    # Create a thread pool executor
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        # Split the 2D array into chunks for parallel processing
        chunks = np.array_split(arr, num_threads, axis=1)

        # Submit the tasks to the executor
        results = executor.map(sum_chunk, chunks)

    # Concatenate the results into a single 2D array
    result = np.hstack(list(results))

    return result


def multithreaded_mean(arr, num_thread):
    def mean_chunk(chunk):
        return np.mean(chunk, axis=-1, dtype=np.float32)

    with ThreadPoolExecutor(max_workers=num_thread) as executor:
        chunks = np.array_split(arr, num_thread, axis=1)
        results = executor.map(mean_chunk, chunks)

    result = np.hstack(list(results))

    return result
