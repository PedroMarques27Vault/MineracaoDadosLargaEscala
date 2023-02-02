from datetime import datetime
import gzip
import sys
import zipfile

class Decaying_Windows:

    def __init__(self, c, t, threshold):
        # Constant should be 10^-6 to 10^-9
        self.c = c
        # Interval in seconds to check current status
        self.t = t
        self.time_counter = 0
        self.last_timestamp = 0
        # Threshold to drop tags/scores
        self.threshold = threshold
        # Tag : (timestamp, score)
        self.tags = {}

        self.frame_counter=0

    def update(self, tag, timestamp):

        for t in self.tags:
            self.tags[t]*=(1-self.c)
            if t == tag:
                self.tags[t]+=1

        if tag not in self.tags:
            self.tags[tag] = 1

        # First go around
        if self.last_timestamp == 0:
            self.last_timestamp = timestamp
            return
        
        self.time_counter += timestamp - self.last_timestamp
        self.last_timestamp = timestamp

        if self.time_counter >= self.t:
            self.time_counter = 0
            self.show_popular()


    def show_popular(self):

        # The procedure of dropings tags should be done on each update but to save execution time
        # We are doing it only on each time frame defined

        # Drop tags with a threshold on the score value
        #self.tags = {key:val for key, val in self.tags.items() if val >= self.threshold}

        # Drop tags with a threshold of maximum number of tags
        self.tags = {key:val for key, val in sorted(self.tags.items(), key=lambda x: x[1], reverse=True)[:self.threshold] }

        popular = sorted(self.tags.items(), key=lambda x: x[1], reverse=True)[:10]

        self.frame_counter+=1
        print(f"Time frame number: {self.frame_counter}")
        print(f'10 most popular tags in the last {self.t} seconds:\n')

        for p in popular:
            print(f' {p[0]} with score {p[1]}')
        print('\n')


if __name__ == "__main__":
    #   Argument Handling
    #   Example of execution python3 decaying_windows.py -c 6 -t 3600 -h 700
    params = ["-c","-t","-h"]
    if len(set([x for x in params if x in sys.argv])) !=3:
        print("Incorrect Parameters\n")
        print("Use: \n-c exponent for c constant (10^-c)\n-t timeframe size in seconds\n-h threshold of tags to keep\n")
        sys.exit(-1)
        

    for i in range(6):
        if sys.argv[i+1] == "-c":
            c = int(sys.argv[i+2])
        elif sys.argv[i+1] == "-t":
            t = int(sys.argv[i+2])
        elif sys.argv[i+1] == "-h":
            threshold = int(sys.argv[i+2])
    
    dw = Decaying_Windows(10**-c, t, threshold)


    for z in ["./mdle_twitter_data/2020.txt.gz", "./mdle_twitter_data/2021.txt.gz"]:
        with gzip.open(z, 'rb') as file:
            while True:
                line = file.readline()
                if not line:
                    break
                line = line.decode(encoding='utf-8')

                data = line.split(" ", 1)
                ts,tags = int(datetime.strptime(data[0], "%Y-%m-%dT%H:%M:%S.000Z").timestamp()), eval(data[1])
                
                for tag in tags:
                    dw.update(tag,ts)
