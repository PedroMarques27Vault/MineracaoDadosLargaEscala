import glob
import shutil
from pyspark import SparkContext
from datetime import datetime
import re
import sys
import random
import os

#   This script contains the implementation of LSH algorithm using shingling and min hashing in order to find similar movies
#   based on their plots

#   Overall flow of execution
#   Transform plots into sets of shingles of size k words
#   Convert sets into smaller signatures of bands*rows size - min hashing 
#   Compare signatures using Jaccard similarity

#References: https://www.pinecone.io/learn/locality-sensitive-hashing/
#2147483647 2^31


class MoviesLsh:

    def __init__(self, movies_filename, plot_filename, output,k, no_bands, no_r, mv):
        self.spark = SparkContext(appName = "MoviesLsh")
        self.movies_filename = movies_filename              # file with movies and their codes
        self.plot_filename = plot_filename                  # file with plots
        self.output_folder = "moviesresults"                # folder to output data to
        self.output_file = output                           # file to output results to
        self.k = int(k)                                     # Number of words in shingle
        self.no_bands = int(no_bands)                       # Number of bands used in LSH
        self.no_rows = int(no_r)                            # number of rows used in LSH
        self.no_hash = self.no_rows * self.no_bands         # number of hash functions
        
        self.load_movies()                                  
        self.load_plot()
        
        
        if "-lm" not in sys.argv:                           # -lm means load from memory. LSH is not executed, only the results are loaded from memory
            self.clear_output_folder()                      # clear results output folder
            shingles = self.generate_shingles()             # start generating shingles
            self.min_hash(shingles)                         # create smalled signatures
            self.lsh_algorithm()                            # apply LSH


        self.load_candidate_pairs()                         # load results from results folder
        
        
        # Print and save to file similar Movies to the one given
        f = open(self.output_file, "a+") 
        f.write("\n"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
        f.write("\nFinding movies similar to {0}".format(mv))
        for x in list(self.get_movies_by_name(mv.lower())):
            print("\nMovies {0} and {1} are {2}% similar".format( x[0],x[1],x[2]*100))
            f.write("\n\t{0} and {1} are {2}% similar".format(x[0],x[1],x[2]*100))
        print("\n\n")   
        
        self.close()
    
    # Clear output folder and delete all of its contents
    def clear_output_folder(self):
        if os.path.exists(self.output_folder):
            shutil.rmtree(self.output_folder)
        os.mkdir(self.output_folder)

    # Load movies from the movies_filename file and create code:movie_name associations
    def load_movies(self):
        textfile = self.spark.textFile(self.movies_filename)
        
        # Split file by lines
        # Remove invalid lines
        # Split line by tab and save code:movie assoc
        self.record = textfile.flatMap(lambda line: re.split(r'\n', line.lower()))\
            .filter(lambda line: len(line.split("\t"))>= 3)\
            .map(lambda movie : (movie.split("\t")[0],movie.split("\t")[2]))\
            .sortBy(lambda p: p[0])
    
    
    # Load plot of the movies from the plot_filename file and create code:plot associations
    def load_plot(self):
        textfile = self.spark.textFile(self.plot_filename)
        # Split file by lines
        # Remove invalid lines
        # Split line by tabs and save code:plot assoc
        self.conditions = textfile.flatMap(lambda line: re.split(r'\n', line.lower())) \
                                .filter(lambda line: len(line.split("\t"))== 2)\
                                .map(lambda plot : (plot.split("\t")[0],plot.split("\t")[1]))\
                                .sortBy(lambda p: p[0])
    
    # Using the plots stored in self.conditions, create shingles of size k words
    # Words are defined as segments of chars enclosed by whitespace
    # returns dictionary with keys = movie_codes and value  = array_of_shingles
    def generate_shingles(self):
        shingles = dict()
        for movie, plot in self.conditions.collect():
            words = plot.split(" ")                                                                 # Split plot by words
            if len(words)>self.k : shingles[movie] = [' '.join(words[i:i+self.k]) for i in range(len(words)-self.k)]
        self.conditions.unpersist()                                                                 # Conditions RDD is no longer needed so memory can be freed
        return shingles
    
    # Apply min hashing in order to generate smaller signatures
    # creates a global dictionary with keys = movie_names and values = signature
    def min_hash(self, shingles):
        # Generate rows*bands integers a row*bands integers b to create rows*bands hash functions 
        hash_functions = [(random.randint(0,self.no_hash**2),random.randint(0,self.no_hash**2)) for i in range(self.no_hash)]
        
        N = 2**16                                                                                   # Large N to reduce collisions
        p = 2147483647                                                                              #2**31-1 Prime Number -> Mersenne prime (2**n - 1)
        self.signatures = dict()
        for movie, shingleslist in shingles.items():
            # The signature is formed by the minimum hash value of all the hashed shingles for each hash function
            # bands = 2,rows = 3, something of the sort: movie = [12, 26, 1, 9, 50, 32]
            self.signatures[movie] = [min([ ((a*hash(x) + b)% p)% N for x in shingleslist]) for (a,b) in hash_functions]
  


    # Apply LSH algorithm to hash signatures to same bucket, thus finding simillar movies
    def lsh_algorithm(self):
        # Size of the bucket array
        kbuckets = 2**16
        # List of all candidate pairs
        self.candidate_pairs = []
        
        for i in range(self.no_bands):                                                              # Find Similar movies in each band
            
            index = i*self.no_rows                                                                  # Signature pointer
            buckets = [[] for _ in range(kbuckets)]                                                 # Create buckets array
            for movie, signature in self.signatures.items():
                sequence = signature[index:index+self.no_rows]                                      # Iterate over signature, create sequences of size rows
                buckets[hash(str(sequence))%kbuckets].append(movie)                                 # Add movie to hashed location in buckets
            for candidates in buckets:
                # Now that we have all the similar movies in this band, generate all possible pairs in each bucket in the buckets array
                c = list(set(candidates))
                pairs = [c[i:i+2] for i in range(len(c)) ]                                         
                
                # Save all the pairs and their jaccard similarity. Remove all non pairs
                self.candidate_pairs+=[(x, self.jaccard(self.signatures[x[0]], self.signatures[x[1]])) for x in pairs if len(x)==2] 
        
        cp = self.spark.parallelize(self.candidate_pairs)                                           # Turn into RDD
        cp.sortBy(lambda p: -p[1])                                                                  # Sort by similarity
            


        lines = cp.map(lambda data: ';'.join(str(d) for d in data))                                 # map to CSV format

        lines.saveAsTextFile("{0}/{1}".format(self.output_folder, "candidate_pairs"))               # Save in outputfolder/candidate_pairs

    # Load all candidate pairs from memory
    # Load files in outputfolder/candidate_pairs
    # Loads all movie pairs and their jaccard similarity
    def load_candidate_pairs(self):
        if os.path.exists("{0}/{1}".format(self.output_folder, "candidate_pairs") ):
            files = ["{0}/{1}/{2}".format(self.output_folder, "candidate_pairs",f) for f in os.listdir("{0}/{1}".format(self.output_folder, "candidate_pairs")) if 'part-' == f[:5]]
            extfile = self.spark.textFile(','.join(files))
             
            # Load CSV saved as txt file
            self.similarity = extfile.flatMap(lambda line: re.split(r'\n', line.lower())) \
                                    .filter(lambda line: len(line.split(";"))== 2)\
                                    .map(lambda pair : ((pair.split(";")[0]).strip("']['").split("', '"),float(pair.split(";")[1]))) \
                                    .sortBy(lambda p: p[0])
            
    # Given a movie name, return all similar movies with similarity > 0.8 
    # Remove equal movies by topping similarity at 0.98
    # Returns a set of similar movies and their jaccard similarity to the one given
    def get_movies_by_name(self, mv):
        code = self.record.filter(lambda p: p[1] == mv).take(1)[0][0]                                   # Find code for given movie
        
        similar = set()
        for x,y in self.similarity.filter(lambda p: code in p[0] and p[1]>0.8 and p[1]<0.98).collect():
            # Save these similar movies
            sim = [c for c in x if c != code][0]
            similar.add((mv, self.record.filter(lambda p: p[0] == sim).take(1)[0][1], y))
  
        return similar
    
    # Calculate the jaccard similarity
    def jaccard(self,a, b):
        return len(set([x for x in a if x in b])) / len(set(a+b))

    def close(self):
        self.spark.stop()


#   Argument Handling
#   Example of execution movies.py -mf movie.metadata.tsv -pf plot_summaries.txt -o results -k 3 -b 27 -r 3 -mv "rocky pink"
#   Optional: 
#       -lm: Loads LSH data from memory instead of doing the whole process
#   A Combination that finds at least 99.5% of pairs with 80% similarity and less than 5% of pairs with 40%: 
#       b = 27, r = 3
if __name__ == "__main__":
    
    if len(sys.argv)!=15 and len(sys.argv) != 16:
        print("Only given "+str(len(sys.argv))+" parameters")
        print("Incorrect Parameters\n")
        print("Use: \n-mf movies_filename\n-pf plot_filename\n-o output_file\n-k no_words_in_shingle\n-b number_of_bands\n-r number_of_rows\n-mv movie_name\n\nOptional: -lm -> load from memory")
        print("Suggested Arguments:\nb = 27 and r = 3")
        sys.exit(-1)
    params = ["-mf", "-pf","-o","-k", "-b", "-r", "-mv"]
    
    if len(set([x for x in params if x in sys.argv])) !=7:
        print("Not All Arguments were provided")
        sys.exit(-1)
    _size = len(sys.argv)-1
    for i in range(_size):
        if sys.argv[i+1] == "-mf":          # movie's file filename
            mv_filename = sys.argv[i+2]    
        elif sys.argv[i+1] == "-pf":        # plot's file filename
            plot_filename = sys.argv[i+2]
        elif sys.argv[i+1] == "-o":         # output filename
            out = sys.argv[i+2]
        elif sys.argv[i+1] == "-k":         # size of shingles (number of words)
            k = int(sys.argv[i+2])
        elif sys.argv[i+1] == "-b":         # Number of bands
            b = sys.argv[i+2]
        elif sys.argv[i+1] == "-r":         # Number of rows
            r = int(sys.argv[i+2])
        elif sys.argv[i+1] == "-mv":        # Name of movie to find similar
            mv = sys.argv[i+2]
    movies = MoviesLsh(mv_filename, plot_filename, out, k,b,r, mv)
    
    
    
    
