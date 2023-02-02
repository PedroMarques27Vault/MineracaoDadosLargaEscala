import glob
import itertools
from pkgutil import iter_modules
import shutil
from pyspark import SparkContext
from datetime import datetime
import re
import sys
import os

# This script contains the implementation of the apriori algorithm for finding frequent item sets
class APriori:

    def __init__(self, filename, output, k, threshold):
        self.spark = SparkContext(appName = "APriori")
        self.spark.setLogLevel("ERROR")
        self.filename = filename            # Name of file to with itemsets
        self.output_folder = "apriorik="+k   # Output folder where to store temporary data
        self.output_file = output           # Output file where to save results
        self.k = int(k)                     # Size of the item sets
        self.threshold = int(threshold)     #Frequency threshold
        self.clear_output_folder()      

    # clears the output folder and deletes all of its contents
    def clear_output_folder(self):
        if os.path.exists(self.output_folder):
            shutil.rmtree(self.output_folder)
        os.mkdir(self.output_folder)
    
    # load items from the input file and save the association patient: array_of_conditions
    # This is stored in outputfolder/data 
    def load_baskets(self):
        textfile = self.spark.textFile(self.filename)
        # Split file by lines
        # # Filter out invalid lines
        # Split lines by comma and save pair patient:[condition]
        # join all equal keys
        self.record = textfile.flatMap(lambda line: re.split(r'\n', line.lower()))\
            .filter(lambda line: len(line.split(","))== 6)\
                .map(lambda word : (word.split(",")[2],[word.split(",")[-2]]))\
                    .reduceByKey(lambda a,b: sorted(set(a+b)))\
                        .sortBy(lambda p: p[0])                         

        self.record.saveAsTextFile("{0}/{1}".format(self.output_folder, "data"))
  
    # load condition descriptions from the input file and save the association code: description
    # This is stored in outputfolder/conditions 
    def load_conditions(self):
        textfile = self.spark.textFile(self.filename)
        # split by lines
        # remove invalied lines
        # Split lines by comma and sabe pair condition_code: description
        self.conditions = textfile.flatMap(lambda line: re.split(r'\n', line.lower()))\
            .filter(lambda line: len(line.split(","))== 6)\
                .map(lambda word : (word.split(",")[-2],word.split(",")[-1]))\
                    .reduceByKey(lambda a,b: a).sortBy(lambda p: p[0])
   
        self.conditions.saveAsTextFile("{0}/{1}".format(self.output_folder, "conditions"))
    
    # Count the total occurrence of each item 
    # Return this count 
    def count_frequent(self):
        counts = dict()
        for x in self.record.collect():
            codes = x[1]
            for cod in codes:
                if cod not in counts: counts[cod] = 1
                else: counts[cod]+=1
        return counts
    
    
    # Keep only items above threshold 
    def get_frequent_table(self, counts):
        return {k:v for k,v in counts.items() if v>=self.threshold}
    
    # frequent_table parameter: result of get_frequent_table function: Only items that are considered frequent
    # 2 Cases: K=2, k=3
    def get_frequent_itemsets(self, frequent_table, current_k = 2):
        if current_k == 2: 
            #   K=2: 
            #       Create all possible pairs from the frequent items
            #       Count their occurrences
            itemsets_count = dict()
            for x in self.record.collect():
                codes = [x for x in x[1] if x in frequent_table]       
                pairs = list(itertools.combinations(codes,2))
                for p1 in pairs:
                    if p1 not in itemsets_count: itemsets_count[p1]=1
                    else: itemsets_count[p1]+=1
   
        elif current_k==3:
            #   K=3: 
            #       
            #       Use all frequent itemsets calculated for k = 2 and create trios
            #       All size 2 subsets that can possibly be generated from this trio are frequent
            #       Count their occurrences
            itemsets_c1 = dict()
            
            
            all_combs = list(itertools.combinations(frequent_table,2))
            all_combinations_as_lists = [sorted(set(list(a[0])+list(a[1]))) for a in all_combs]
            all_trios = [(x[0],x[1],x[2]) for x in all_combinations_as_lists if len(x)==3]
            
            print(self.record.count())
            for condition in self.record.collect():
                combinationsR =[(f[0],f[1],f[2]) for f in [sorted(x) for x in list(itertools.combinations(condition[1],3))]]

                for t1 in  combinationsR:
                    if t1 not in itemsets_c1: itemsets_c1[t1]=1
                    else: itemsets_c1[t1]+=1 
              
            itemsets_count = {k:itemsets_c1[k] for k in all_trios if k in itemsets_c1 }
        return itemsets_count
    
    #  Executes the full apriori script
    #  Needs the load_basket and load_conditions functions to have been executed previously
    def apriori(self):
        # first pass
        counts = self.count_frequent()
        frequent_table = self.get_frequent_table(counts)
        #second pass
        frequent = self.get_frequent_itemsets(frequent_table)
     
        if self.k == 2:
            
            #   Save results
            #   Possibly frequent pairs saved in outputfolder/apriori_resultsk=2
            rdd =self.spark.parallelize([(k,v) for k,v in frequent.items()]).sortBy(lambda p:-p[1])
            rdd.saveAsTextFile("{0}/{1}".format(self.output_folder,"apriori_resultsk="+str(self.k) ))
            
            # Print results
            res = rdd.take(10)
            # Print results and save to file
            print("For k=2, Most frequent:\n")
            res = rdd.take(10)
            
            f = open(self.output_file, "a+") 
            f.write("\n"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
            f.write("\nMost frequent itemsets of size k = 2, support threshold = {0}".format(self.threshold))
            results = []
            for i in range(10):
                # Turn Codes Into Descriptions
                c1= self.conditions.filter(lambda p: p[0] == res[i][0][0]).take(1)[0][1]
                c2 = self.conditions.filter(lambda p: p[0] == res[i][0][1]).take(1)[0][1]
                results.append("[ {0}, {1}]".format(c1,c2))
            for x in results:
                print(x)
                f.write("\n {0}".format(x))
            f.close()
        elif self.k == 3:
            #   Save results
            #   Possibly frequent pairs saved in outputfolder/apriori_resultsk=2
           
            frequent_table = self.get_frequent_table(frequent)
            frequent2 = self.get_frequent_itemsets(frequent_table, 3)
            rdd =self.spark.parallelize([(a,b) for a,b in frequent2.items()]).sortBy(lambda p:-p[1])
            rdd.saveAsTextFile("{0}/{1}".format(self.output_folder,"apriori_resultsk="+str(self.k) ))
            
            # Print results and save to file
            print("For k=3, Most frequent:\n")
            res = rdd.take(10)
            
            f = open(self.output_file, "a+") 
            f.write("\n"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
            f.write("\nMost frequent itemsets of size k = 3, support threshold = {0}".format(self.threshold))
            results = []
            for i in range(10):
                # Turn Codes Into Descriptions
                c1= self.conditions.filter(lambda p: p[0] == res[i][0][0]).take(1)[0][1]
                c2 = self.conditions.filter(lambda p: p[0] == res[i][0][1]).take(1)[0][1]
                c3 = self.conditions.filter(lambda p: p[0] == res[i][0][2]).take(1)[0][1]
                results.append("[ {0}, {1}, {2}]".format(c1,c2,c3))
            for x in results:
                print(x)
                f.write("\n {0}".format(x))
            f.close()
            
            return 0
        return -1

    def close(self):
        self.spark.stop()


if __name__ == "__main__":
    #   Argument Handling
    #   Example of execution apriori.py -f conditions.csv -o results -k 2 -t 1000
    params = ["-f", "-o","-k", "-t"]
    if len(set([x for x in params if x in sys.argv])) !=4:
        print("Incorrect Parameters\n")
        print("Use: \n-f filename\n-o output_file\n-k size_of_baskets\n-t threshold")
        sys.exit(-1)
        
        
    for i in range(8):
        if sys.argv[i+1] == "-f":
            filename = sys.argv[i+2]
        elif sys.argv[i+1] == "-o":
            out = sys.argv[i+2]
        elif sys.argv[i+1] == "-k":
            k = sys.argv[i+2]
        elif sys.argv[i+1] == "-t":
            thresh = sys.argv[i+2]

    
    apriori = APriori(filename,out,k,thresh)
    apriori.load_baskets()
    apriori.load_conditions()
    apriori.apriori()
    apriori.close()
