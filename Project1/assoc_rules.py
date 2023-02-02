import glob
import shutil
from pyspark import SparkContext
from datetime import datetime
from apriori import APriori
import re
import sys
import os
import itertools

# This script relies on the apriori algorithm to get frequent item sets and generate association rules of X -> Y and X,Y -> Z
class Rule_Finder:

    def __init__(self, filename, output, k, threshold):
        self.spark = None
        self.frequent_singles = None
        self.frequent_pairs = None
        self.frequent_trios = None
        self.rules = []
        self.filename = filename
        self.output_folder = "assocrules"
        
        self.apriori = APriori(filename, "", k, threshold)
        self.clear_output_folder()

    def clear_output_folder(self):
        if os.path.exists(self.output_folder):
            shutil.rmtree(self.output_folder)
        os.mkdir(self.output_folder)

    # Getting all the required data to generate the rules, all frequent items, frequent pairs and frequent trios
    def set_up(self):
        self.apriori.load_baskets()
        self.apriori.load_conditions()
        

        counts = self.apriori.count_frequent()
        self.frequent_singles = self.apriori.get_frequent_table(counts)
        self.frequent_pairs = self.apriori.get_frequent_itemsets(self.frequent_singles)


        pair_table = self.apriori.get_frequent_table(self.frequent_pairs)
        self.frequent_trios = self.apriori.get_frequent_itemsets(pair_table, 3)
        
        self.total_baskets = self.apriori.record.count()
        
        self.apriori.close()
        self.spark = SparkContext(appName = "AssocRules")

    #Function that will generate rules of type X -> Y from the frequent pairs
    def get_single_rules(self):
        # Iterate pairs and then combinations of each pair
        for pair in self.frequent_pairs:
            for c in itertools.permutations(pair,2):

                confidence, interest, lift, std_lift = self.calc_XY_stats(c[0],c[1],pair)
                # Only add to rule list if the rulling std lift is 0.2 or more
                if std_lift >= 0.2:
                    self.rules.append({'rule':(c[0]+' -> '+c[1]), 'confidence':confidence, 'interest':interest, 'lift':lift, 'std_lift':std_lift, })


    #Function that will generate rules of type X,Y -> Z from the frequent trios
    def get_pair_rules(self):
        # Iterate trios and then combinations of each trio
        for trio in self.frequent_trios:
            x = trio[0]
            y = trio[1]
            z = trio[2]
            for c in [(x,y,z), (x,z,y), (y,z,x)]:

                confidence, interest, lift, std_lift = self.calc_XYZ_stats(c[0],c[1],c[2],trio)
                # Only add to rule list if the rulling std lift is 0.2 or more
                if std_lift >= 0.2:
                    self.rules.append({'rule':(c[0]+','+c[1]+' -> '+c[2]), 'confidence':confidence, 'interest':interest, 'lift':lift, 'std_lift':std_lift, })


    # Using frequent pairs and single items, gets the confidence, interest, lift and std lift of a given pair of items as an association
    def calc_XY_stats(self, x, y, order):
        
        support_x = self.frequent_singles.get(x)
        support_y = self.frequent_singles.get(y)
        support_xy = self.frequent_pairs.get(order)

        confidence = support_xy/support_x

        prob_x = support_x/self.total_baskets
        prob_y = support_y/self.total_baskets

        interest = confidence - prob_y

        lift = confidence/prob_y

        std = max(prob_x+prob_y-1, 1/self.total_baskets)
        std_lift = (lift - std) / ( (1/max(prob_x,prob_y)) - std )

        return round(confidence,3), round(interest,3), round(lift,3), round(std_lift,3)


    # Using frequent pairs and trios items, gets the confidence, interest, lift and std lift of a given trio of items as an association
    def calc_XYZ_stats(self, x, y, z, order):
        
        support_xy = self.frequent_pairs.get((x,y)) if (x,y) in self.frequent_pairs else self.frequent_pairs.get((y,x))
        if support_xy == None:
            return 0,0,0,0
        support_z = self.frequent_singles.get(z)
        support_xyz = self.frequent_trios.get(order)

        confidence = support_xyz/support_xy

        prob_xy = support_xy/self.total_baskets
        prob_z = support_z/self.total_baskets

        interest = confidence - prob_z

        lift = confidence/prob_z

        std = max(prob_xy+prob_z-1, 1/self.total_baskets)
        std_lift = (lift - std) / ( (1/max(prob_xy,prob_z)) - std )

        return round(confidence,3), round(interest,3), round(lift,3), round(std_lift,3)


    #writes rules to results file
    def write_rules(self):
        rdd =self.spark.parallelize(self.rules).sortBy(lambda d: -d['std_lift'])
        rdd.saveAsTextFile("{0}/{1}".format(self.output_folder,"rules_results" ))

    def close(self):
        self.spark.stop()



if __name__ == "__main__":
    #   Argument Handling
    #   Example of execution apriori.py -f conditions.csv -o results -k 2 -t 1000
    params = ["-f", "-o","-k", "-t"]
    if len(set([x for x in params if x in sys.argv])) !=4:
        print("Incorrect Parameters\n")
        print("Use: \n-f filename\n-o output_folder\n-k size_of_baskets\n-t threshold")
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

    rule_finder = Rule_Finder(filename, out, k, thresh)
    rule_finder.set_up()
    rule_finder.get_single_rules()
    rule_finder.get_pair_rules()
    rule_finder.write_rules()
    rule_finder.close()
