import glob
import itertools
import logging
import math
import multiprocessing
from pkgutil import iter_modules
import queue
import random
import shutil
import threading
import time
from pyspark import SparkConf, SparkContext, StorageLevel
import pyspark as sp
from datetime import datetime
import re
import sys
from scipy import spatial
import os
from itertools import combinations
import more_itertools as mit
# The queue for tasks


# Worker, handles each task



# This script contains the implementation of the apriori algorithm for finding frequent item sets
class CFItemItem:

    def __init__(self, rat_file, rec_output, sim_threshold,  part_size = 100000):
        
        self.spark = SparkContext(appName = "CFItemItem", conf = SparkConf().set("spark.driver.memory","10g").set("spark.executor.memory","10g"))
        self.spark.setLogLevel("ERROR")
        
        self.rat_file = rat_file           
        self.output_folder = "CFItemItem"   
        self.output_file = rec_output       # Output file where to save results
        self.sim_threshold = float(sim_threshold)
        self.movie_ratings = dict()
        self.movie_ratings_userlist = dict()
        self.partition_size = part_size
        self.lock = threading.Lock()

    # clears the output folder and deletes all of its contents
    def clear_output_folder(self):
        if os.path.exists(self.output_folder):
            shutil.rmtree(self.output_folder)
        os.mkdir(self.output_folder)
                      

  
    def load_movie_ratings(self): #Key = movieId, values = [(userId, Rating)...] 
        textfile = self.spark.textFile(self.rat_file)
        self.ratings = textfile.flatMap(lambda line: re.split(r'\n', line.lower()))\
            .filter(lambda line: len(line.split(","))== 4 and "movieid" not in line)\
                .map(lambda line : (line.split(",")[1],[(int(line.split(",")[0]), float(line.split(",")[2]))]))\
                    .reduceByKey(lambda a,b: sorted(set(a+b), key = lambda p: p[0]))\
                    .sortBy(lambda p: p[0])
     
        users_ratings = [p[1] for p in self.ratings.collect()]
        ids = [x[0] for k in users_ratings for x in k ]
        self.max_user_id = max(ids)
        
    def load_ratings_by_user(self):
        textfile = self.spark.textFile(self.rat_file)
        self.ratings_by_user = textfile.flatMap(lambda line: re.split(r'\n', line.lower()))\
            .filter(lambda line: len(line.split(","))== 4 and "movieid" not in line)\
                .map(lambda line : (int(line.split(",")[0]),[(int(line.split(",")[1]), float(line.split(",")[2]))]))\
                    .reduceByKey(lambda a,b: sorted(set(a+b), key = lambda p: p[0]))\
                    .sortBy(lambda p: p[0])
                    
    def load_similarity(self):
        all_files_to_load = [x[0]+"/part-00000" for x in os.walk("CFItemItem/similarities")][1:]
 
        extfile = self.spark.textFile(','.join(all_files_to_load))
            
        # Load CSV saved as txt file
        self.similarities = extfile.flatMap(lambda line: re.split(r'\n', line.lower())) \
                                .filter(lambda line: len(line.split(","))== 3 )\
                                .map(lambda pair : (int(pair.split(",")[0]),int(pair.split(",")[1]),float(pair.split(",")[2]))) \
                                    .sortBy(lambda p: p[1])


        
    def create_movie_ratings_arrays(self):
       
        
        for movie, m_rating_list in self.ratings.collect():
            ratings = {k[0]:k[1] for k in m_rating_list}
            avg = sum(ratings.values())/len(ratings)
            self.movie_ratings[movie] = [ratings[u+1]-avg if u+1 in ratings else 0 for u in range(self.max_user_id)]
            self.movie_ratings_userlist[movie] = [k[0] for k in m_rating_list]
    
    def saveRdd(self, sims, fileN):
        similarities = self.spark.parallelize(sims).sortBy(lambda p: p[2])
        lines =  similarities.map(lambda data: ','.join(str(d) for d in data))    # map to CSV format
        
        lines.coalesce(1).saveAsTextFile("{0}/{1}".format(self.output_folder, "similarities/partition"+str(int(fileN))))        
            
    def calculate_cosine_similarity(self):
        print("Starting Cosine Similarity Calculator")
        sims = []
        
        mv_list = list(self.movie_ratings_userlist.keys())
        
   
        all_movie_combs = list(combinations(mv_list,2))
        t = time.time()
        i= 0
        print(len(all_movie_combs))
        for (m1,m2 ) in all_movie_combs:
            intersection = [x for x in self.movie_ratings_userlist[m1] if x in  self.movie_ratings_userlist[m2]]
            if len(intersection)>0:
                lu1 = [self.movie_ratings[m1][int(i)-1] for i in intersection]
                lu2 = [self.movie_ratings[m2][int(i)-1] for i in intersection]
           
                sims += [(m1,m2, 1-spatial.distance.cosine(lu1,lu2))]
                if i%self.partition_size==0: 
                    if (i!=0): self.saveRdd(sims, (i/self.partition_size))
                    sims = []
                    print(i,time.time()-t)
                i+=1
        
        print("Gathered Intersections", time.time()-t)
        fn = math.ceil(i/100000)
        self.saveRdd(sims, fn)
        print("Finished in ",time.time()-t)
    

      
        
            
    
    def estimate_rating(self):
        if os.path.exists("{0}/{1}".format(self.output_folder,self.output_file)):
            shutil.rmtree("{0}/{1}".format(self.output_folder,self.output_file))
        self.estimate = []
        
        has_similars = []
        sims = dict()
        self.results = dict()
        for x in self.similarities.collect():
            if x[2]>self.sim_threshold:
                has_similars += [x[0],x[1]]
                sims[(x[0],x[1])] = x[2]
                sims[(x[1],x[0])] = x[2]
                
                
        print("Started Collecting Data")
        dataset = self.ratings_by_user.collect()
        t2 = time.time()
        
        sims_items = sims.items()
        print("Started Calculating Estimations")
        i=0
        for user, ratings in dataset:
            # for each user
            # Get All movies which have similarities movie:rating
            t1 = time.time()
            ratings_by_movie = {int(k[0]):k[1] for k in ratings if int(k[0]) in has_similars}
            # Get Similarities between rated movies and unrated movies (rated,unrated) = similarity
            filtered = {k:v for k,v in sims_items if k[0] in ratings_by_movie and k[1] not in ratings_by_movie}
            _set = list(set([k[1] for k in filtered]))
            #Set of all unrated movies which can be estimated
            for movie in _set:
                # Get all movies rated by user which are similar to movie
                can_count = [m for m in ratings_by_movie if (m,movie) in filtered]
                # If there is more that 0 movies
                if len(can_count)>0:
                    weighted_sum = sum([filtered[(m, movie)]*ratings_by_movie[m] for m in can_count])
                    sum_of_weights = sum([filtered[(m, movie)] for m in can_count])
                    rating = weighted_sum/sum_of_weights 
                    self.estimate.append((movie, user, rating))
            print("User",i,": estimated", len(_set),"ratings in ", time.time()-t1,"seconds")
            

        
        print("Finished Calculating Results", time.time()-t2)
       
        
            
        resultsRdd = self.spark.parallelize(self.estimate).sortBy(lambda p: p[0])
        lines =  resultsRdd.map(lambda data: ';'.join(str(d) for d in data))    # map to CSV format
        lines.saveAsTextFile("{0}/{1}".format(self.output_folder,self.output_file)) 
        
    def close(self):
        self.spark.stop()


if __name__ == "__main__":
    #   Argument Handling
    params = ["-rf", "-o","-t"]
    if len(set([x for x in params if x in sys.argv])) !=3:
        print("Incorrect Parameters\n")
        print("Use: \n-rf Filename_of_User_Ratings(String)\n-o recommendation_output_file(String)\n-t similarity_threshold(Double/Float)")
        sys.exit(-1)
    partSize = 100000
    for i in range(8):
        if sys.argv[i+1] == "-rf":
            r_filename = sys.argv[i+2]
            
        elif sys.argv[i+1] == "-o":
            out = sys.argv[i+2]
  
        elif sys.argv[i+1] == "-t":
            thresh = sys.argv[i+2]
        
        elif sys.argv[i+1] == "--partition-size":
            partSize = int(sys.argv[i+2])

    lm = False
    if "-lm" in sys.argv:
        lm = True
        
        
    cfItemItem = CFItemItem(r_filename, out, float(thresh), partSize)
    
    cfItemItem.load_movie_ratings()
    cfItemItem.create_movie_ratings_arrays()
    if not lm:
        cfItemItem.clear_output_folder()      
        cfItemItem.calculate_cosine_similarity()
    cfItemItem.load_similarity()
    cfItemItem.load_ratings_by_user()
    cfItemItem.estimate_rating()
    
    cfItemItem.close()
    
    
    
