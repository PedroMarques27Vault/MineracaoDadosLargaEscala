from cProfile import label
from collections import deque
import matplotlib.pyplot as plt
import math
import random
import sys

# The class that implements the dgim algorithm
class DGIM:

    def __init__(self, N):
        # Size of window
        self.N = N
        self.max_buckets = 2

        self.timestamp = 0
        # Time stamp of the oldest bucket
        self.oldest_timestamp = -1

        # Using deques to easily take from each side
        # Each deque in the list holds the buckets of 2 to the power of the list index
        self.deques = []

        self.max_index = int(math.ceil(math.log(N)/math.log(2)))

        self.deques = [deque() for d in range(self.max_index + 1)]

    def update(self, bit):

        # So timestamps don't get uncessarily big, keep them at 2 times N at most
        self.timestamp = (self.timestamp + 1) % (2 * self.N)

        # Check if the last bucket is too old
        if ( self.oldest_timestamp >= 0 and ( (self.timestamp - self.oldest_timestamp) % (2 * self.N) >= self.N ) ):
            self.remove_last_bucket()

        # Zeros don't alter buckets
        if bit == 0:
            return

        current_ts = self.timestamp

        if self.oldest_timestamp == -1:
            self.oldest_timestamp = self.timestamp

        for d in self.deques:
            d.appendleft(current_ts)

            # If bucket limit for this power of 2 hasn't been reached then break out
            if len(d) <= self.max_buckets:
                break
            
            last = d.pop()
            second_last = d.pop()

            # Merging last two buckets then keep going
            current_ts = second_last
            if last == self.oldest_timestamp:
                self.oldest_timestamp = second_last

    
    def remove_last_bucket(self):

        # Remove oldest bucket
        for d in reversed(self.deques):
            if len(d) > 0:
                d.pop()
                break

        # Update oldest bucket timestamp
        self.oldest_timestamp = -1
        for d in reversed(self.deques):
            if len(d) > 0:
                self.oldest_timestamp = d[-1]
                break

    # Get an estimated count of all 1s in the window of size N
    def estimate_count(self):

        result = 0
        last_val = 0

        for i in range(len(self.deques)):

            d_length = len(self.deques[i])
            if d_length > 0:
                last_val = 2**i
                result += d_length*last_val
        
        result-= last_val/2
            
        return result

    # Get and estimated count of 1s in the past k elements, k < N
    def query_k(self, k):
        if k >= self.N:
            return None

        result = 0

        for i in range(len(self.deques)):
            for b in reversed(self.deques[i]):
                if abs(self.timestamp - b) >= k:
                    result += (2**i)/2
                    return result
                else:
                    result += 2**i
            
        return result


# Class that always calculates the exact count of 1s at the cost of performance
class True_Counter:

    def __init__(self, N):
        self.N = N
        self.window = deque()

    def update(self, bit):

        self.window.append(bit)

        if len(self.window) > self.N:
            self.window.popleft()

    def exact_count(self):
        return sum(self.window)

    def exact_query(self, k):
        if k >= self.N:
            return None

        result = 0
        for i in range(min(len(self.window),k)):
            result += self.window[i]
        return result


# Separate function to generate a stream of bits
def generate_bit_stream(length):

    for _ in range(length):
        yield random.randint(0,1)


if __name__ == "__main__":
    #   Argument Handling
    #   Example of execution: python3 dgim.py -n 230 -k 20 -l 7000
    params = ["-n","-k","-l"]
    if len(set([x for x in params if x in sys.argv])) !=3:
        print("Incorrect Parameters\n")
        print("Use: \n-n N\n-k query of k size\n-l length\n")
        sys.exit(-1)
        
        
    for i in range(6):
        if sys.argv[i+1] == "-n":
            N = int(sys.argv[i+2])
        elif sys.argv[i+1] == "-k":
            k = int(sys.argv[i+2])
        elif sys.argv[i+1] == "-l":
            length = int(sys.argv[i+2])


    stream = generate_bit_stream(length)
    dgim = DGIM(N)
    counter = True_Counter(N)

    n_arr = []
    tn_arr = []
    k_arr = []
    tk_arr = []

    for bit in stream:
        dgim.update(bit)
        counter.update(bit)

        estimate = dgim.estimate_count()
        real = counter.exact_count()
        query = dgim.query_k(k)
        real_query = counter.exact_query(k)

        n_arr.append(estimate)
        tn_arr.append(real)
        k_arr.append(query)
        tk_arr.append(real_query)

        print(f'True 1s in N : {real} ; Estimate 1s in N : {estimate} ; Real query of k={k} : {real_query} ; Estimate query of k={k} : {query}')

    plt.plot(n_arr, label = "Estimate 1s in N")
    plt.plot(tn_arr, label = "True 1s in N")
    plt.plot(k_arr, label = "Estimate 1s in query of size k")
    plt.plot(tk_arr, label = "True 1s in query of size k")
    plt.legend()
    plt.show()