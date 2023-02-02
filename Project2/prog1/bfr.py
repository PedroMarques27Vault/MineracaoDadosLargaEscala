from email import header
import glob
import itertools
from pkgutil import iter_modules
import shutil
from pyspark import SparkContext
import re
import sys
import os
import pandas as pd
import ast
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.sql import SQLContext
from pyspark.ml.evaluation import ClusteringEvaluator
from math import dist
import numpy as np

class BFR:

    def __init__(self, output):
        self.spark = SparkContext(appName = "BFR")
        self.sqlContext = SQLContext(self.spark)
        self.spark.setLogLevel("ERROR")

        self.load_size = 0
        self.loaded_points = None
        self.current_df = None
        self.DS = {}

        self.output_folder = "bfr" 
        self.output_file = output
        self.clear_output_folder()      

    # clears the output folder and deletes all of its contents
    def clear_output_folder(self):
        if os.path.exists(self.output_folder):
            shutil.rmtree(self.output_folder)
        os.mkdir(self.output_folder)
    

    def load_tracks(self):
        tracks = pd.read_csv("./tracks.csv", index_col=0, header=[0, 1])

        COLUMNS = [('track', 'tags'), ('album', 'tags'), ('artist', 'tags'),
                ('track', 'genres'), ('track', 'genres_all')]
        for column in COLUMNS:
            tracks[column] = tracks[column].map(ast.literal_eval)

        COLUMNS = [('track', 'date_created'), ('track', 'date_recorded'),
                ('album', 'date_created'), ('album', 'date_released'),
                ('artist', 'date_created'), ('artist', 'active_year_begin'),
                ('artist', 'active_year_end')]
        for column in COLUMNS:
            tracks[column] = pd.to_datetime(tracks[column])

        SUBSETS = ('small', 'medium', 'large')
        try:
            tracks['set', 'subset'] = tracks['set', 'subset'].astype(
                    'category', categories=SUBSETS, ordered=True)
        except (ValueError, TypeError):
            # the categories and ordered arguments were removed in pandas 0.25
            tracks['set', 'subset'] = tracks['set', 'subset'].astype(
                    pd.CategoricalDtype(categories=SUBSETS, ordered=True))

        COLUMNS = [('track', 'genre_top'), ('track', 'license'),
                ('album', 'type'), ('album', 'information'),
                ('artist', 'bio')]
        for column in COLUMNS:
            tracks[column] = tracks[column].astype('category')

        return tracks

    def load_points_small(self):
        features = self.spark.textFile("./features.csv")
        
        tracks = pd.read_csv("./tracks.csv", header=1, usecols=[0, 32])
        tracks = tracks.loc[tracks['subset'] == "small"]
        small_id_list = tracks["track_id"].tolist()
        small_id_list = [str(int(i)) for i in small_id_list]

        self.record = features.flatMap(lambda line: re.split(r'\n', line.lower()))\
            .filter(lambda line: line.split(",")[0] in small_id_list)\
                .map(lambda line: line.split(","))

        self.record.saveAsTextFile("{0}/{1}".format(self.output_folder, "data"))
    
    def find_best_k(self):

        df = self.record.toDF()
        
        for col in df.columns:
            if col != "_1":
                df = df.withColumn(col,df[col].cast('float'))
        
        df = df.withColumnRenamed("_1","id")
        
        vecAssembler = VectorAssembler(inputCols=df.columns[1:], outputCol="features")
        df_kmeans = vecAssembler.transform(df).select('id', 'features')

        scores = {}
        evaluator = ClusteringEvaluator()

        for k in range(8,17):
            kmeans = KMeans().setK(k).setSeed(1).setFeaturesCol("features")
            model = kmeans.fit(df_kmeans)
            scores[str(k)] = evaluator.evaluate(model.transform(df_kmeans))

            centers = model.clusterCenters()

            transformed = model.transform(df_kmeans).select('id', 'prediction')
            rows = transformed.collect()
            df_pred = self.sqlContext.createDataFrame(rows)
            df_pred = df_pred.join(df, 'id')

            for c in range(0,k):
                radius = 0
                diameter = 0

                filtered = df_pred.filter(df_pred.prediction == str(c))
                points = filtered.collect()

                for p1 in points:
                    l1 = list(p1)[2:]
                    r = dist(l1,centers[k-8].tolist())
                    if r>radius:
                        radius = r

                    for p2 in points:
                        l2 = list(p2)[2:]
                        d = dist(l1,l2)
                        if d>diameter:
                            diameter = d
                
                density_d = len(points)/pow(diameter,2)
                density_r = len(points)/pow(radius,2)

                print("For k="+str(k)+" c="+str(c)+" | radius = "+str(radius)+" | diameter = "+str(diameter)+" | density_r = "+str(density_r)+" | density_d = "+str(density_d))


        
        for k,s in scores.items():
            print("Scored "+str(s)+" squared euclidean distance for K = "+k)

        best_k = max(scores, key=scores.get)

        print("The best k for the small dataset was: "+best_k)


    def k_means(self, k):
        df = self.loaded_points.toDF()
        
        for col in df.columns:
            if col != "_1":
                df = df.withColumn(col,df[col].cast('float'))
        
        df = df.withColumnRenamed("_1","id")
        
        vecAssembler = VectorAssembler(inputCols=df.columns[1:], outputCol="features")
        df_kmeans = vecAssembler.transform(df).select('id', 'features')

        kmeans = KMeans().setK(int(k)).setSeed(1).setFeaturesCol("features")
        model = kmeans.fit(df_kmeans)

        transformed = model.transform(df_kmeans).select('id', 'prediction')
        rows = transformed.collect()
        df_pred = self.sqlContext.createDataFrame(rows)

        self.current_df = df_pred.join(df, 'id')

        return model.clusterCenters()

    def load_points_batch(self):
        features = self.spark.textFile("./features.csv")

        self.loaded_points = features.flatMap(lambda line: re.split(r'\n', line.lower()))\
            .map(lambda line: line.split(",")[self.load_size:self.load_size+7999])

        return self.loaded_points

    def sumarize_DS(self):
        for k,c in self.DS.items():
            filtered = self.current_df.filter(self.current_df.prediction == str(k))
            points = filtered.collect()



            self.DS[k]["N"] = len(points)
            self.DS[k]["SUM"] = list(np.sum([list(l) for l in points], axis=0))
            self.DS[k]["SUMSQ"] = list(np.sum([[i**2 for i in l] for l in points], axis=0))

            



    def bfr(self, k):
        self.load_points_batch()
        centers = self.k_means()

        for i in range(0,k):
            self.DS[str(i)] = {"center": centers[i], "N": 0, "SUM": 0, "SQSUM": 0}

        self.sumarize_DS()


    def close(self):
        self.spark.stop()


if __name__ == "__main__":
    params = ["-o","-k", "-t"]
    if len(set([x for x in params if x in sys.argv])) !=1:
        print("Incorrect Parameters\n")
        print("Use: \n-o output_folder\n-k size_of_baskets\n-t threshold")
        sys.exit(-1)
        
        
    for i in range(2):
        if sys.argv[i+1] == "-o":
            out = sys.argv[i+2]

    bfr = BFR(out)
    bfr.load_points_small()
    bfr.find_best_k()
    bfr.close()
