
import glob
import logging
import itertools
import math
import shutil
from pyspark import SparkConf, SparkContext, StorageLevel
import pyspark as sp
from sklearn.cluster import KMeans
import re
import sys
import os
from pyspark.sql import SparkSession
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy.sparse import csgraph
from sklearn.metrics.cluster import adjusted_rand_score
from collections import Counter

class SGP:

    def __init__(self, file, output_file):
        self.filefolder = file                          # folder of facebook files
        self.output_folder = output_file                # folder where to store results
        self.weights = None                             # dictionary with node weights
        self.cluster_number = dict()                    # number of clusters per user graph
        self.clear_output_folder()

     # Clear/Create results folder
    def clear_output_folder(self):
        if os.path.exists(self.output_folder):
            shutil.rmtree(self.output_folder)
        os.mkdir(self.output_folder)


    # Load graoh from edge file, create adjacency matrix and generate laplacian matrix
    # If weights option is activated, fill adjacency matrix with edge weight 
    def load_networks_laplacian_matrix(self):
        self.matrix = dict()
        self.network_users = dict()
        self.matrix_rows = dict()
        for file in os.listdir(self.filefolder):
            if file.endswith(".edges"):
                edge_file = open(self.filefolder+"/"+file,"r").read()
                values = sorted(set([int(k) for k in edge_file.replace("\n"," ").split(" ") if len(k)!=0]))
                _size = len(values)
                key = int(file.split(".")[0])
                self.matrix_rows[key] = {values[i]:i for i in range(_size)}
                adj_matrix = np.zeros((_size, _size))
                self.matrix[key] = adj_matrix
                diagonal = np.zeros((_size, _size))
                for line in edge_file.split("\n"):
                    if len(line.split(" ")[0])!=0 and len(line.split(" ")[1])!=0:
                        a = self.matrix_rows[key][int(line.split(" ")[0])]
                        b = self.matrix_rows[key][int(line.split(" ")[1])]
                        value = 1
                        if self.weights is not None and int(line.split(" ")[0])!=int(line.split(" ")[1]):
                            value = self.weights[int(line.split(" ")[0])][int(line.split(" ")[1])]
                        adj_matrix[a][b] = value
                        adj_matrix[b][a] = value
            
                for i in range(_size):
                    diagonal[i][i] = sum(adj_matrix[i])
                self.network_users[key] =csgraph.laplacian(adj_matrix, normed=True)
    
    # Decompose the laplacian matrix of every main user
    # results (eigenvalues and eigenvectors) are saved in laplacian_decomp dictionary
    def laplacian_eigendecomposition(self):
        self.laplacian_decomp = dict()
        for key in self.network_users:
            eigenvalues, eigenvectors = np.linalg.eig(self.network_users[key])
            self.laplacian_decomp[key] = (eigenvalues, eigenvectors)

    # Show Cluster size recommendations
    # Print top 20 recommendations based on difference sorted between eigenvalues and the number of clusters
    # Higher Difference and Lower Number of Clusters is preferred
    def get_cluster_number(self):
        for key in self.network_users:
            _sorted = sorted(self.laplacian_decomp[key][0])
            dif = [(k,k+1, _sorted[k+1]-_sorted[k]) for k in range(len(_sorted)-1)]
            
            _sorted = sorted(dif, key = lambda p: p[1])[:20]
            print(f"Cluster Size Suggestions for user {key}\n")
            print(key,sorted(_sorted, key = lambda p:  p[2], reverse = True)[:20])
            self.set_cluster_number(key)

    # Set the number of clusters for the graph k
    def set_cluster_number(self, k):
        self.cluster_number[k] = int(input(f"Cluster Size for key {k}? "))
    
    # Create clustering using K Means algorithm
    # Kmeans is trained with first n eigenvectors, n is the number of clusters
    # Clusters are saved in a file
    def create_clusters(self):

        self.network_clusters = dict()
        self.final_clustering = dict()
        for key in self.network_users:
            open(f"{self.output_folder}/ego{key}.txt","w")
            f = open(f"{self.output_folder}/ego{key}.txt","a+")
            n = self.cluster_number[key]
            X = self.laplacian_decomp[key][1].real[:n]
            kmeans = KMeans(n_clusters=n, random_state=0).fit(X)  
            
            res = kmeans.predict(self.matrix[key])

            mappings = {h:[] for h in range(len(res))}
        
            inversed_matrix_rows = {index:value for (value, index) in self.matrix_rows[key].items()}
            for i in range(len(res)):
                mappings[res[i]]+= [inversed_matrix_rows[i]]
            for cluster in range(n):
                f.write(f"Cluster{cluster}: {(' ').join([str(j) for j in mappings[cluster]])}\n")
            f.close()
            self.final_clustering[key]=mappings
    # Loads the true clusters for each main user by reading the .circles files
    def load_true_labels(self):
        self.labeling = dict()
        for file in os.listdir(self.filefolder):
            
            _labels = []
            _clients_added = []
            if file.endswith(".circles"):
                circle_file = open(self.filefolder+"/"+file,"r").readlines()
                for line in circle_file:
                    key = line.split("\t")[0].replace("circle","")
                    key = int(key)
                    _labels+=[(key, int(user)) for user in line.split("\t")[1:] if int(user) not in _clients_added]
                    _clients_added+=[int(user) for user in line.split("\t")[1:]]
                self.labeling[int(file.split(".")[0])] = _labels

    # Calculate the Adjusted rand score using the labels from the .circles files and the predicted clusters
    def calculate_adj_rand_score(self):
        open(f"{self.output_folder}/scores.txt","w")
        f = open(f"{self.output_folder}/scores.txt","a+")
        for k in self.labeling:
            clients_labeled = [x[1] for x in self.labeling[k]]
            
            clients_predicted = []
            pred_labels_temp = []
            for cluster in self.final_clustering[k]:
                clients_predicted+=self.final_clustering[k][cluster]
                pred_labels_temp+=[(cluster, client) for client in self.final_clustering[k][cluster]]
            intersection = set(clients_labeled).intersection(set(clients_predicted))
            
            true_labels = [m[0] for m in sorted(self.labeling[k], key = lambda p: p[1]) if m[1] in intersection]
            pred_labels = [m[0] for m in sorted(pred_labels_temp, key = lambda p:p[1]) if m[1] in intersection]
            
            f.write(f"Score {k} = {adjusted_rand_score(true_labels,pred_labels)}\n")
        f.close()

    # Load weights for each edge. These weights are the jaccard distance between the features of each user
    def load_weights(self):
        self.weights = dict()
        egoFeatFiles = [file for file in os.listdir(self.filefolder) if ".egofeat" in file or ".feat" in file and "name" not in file]
        all_features = []
        for file in egoFeatFiles:
            if ".feat" in file:
                all_features+=[(int(line.split(" ")[0]), [i for i in range(len(line.split(" ")[1:])) if int(line.split(" ")[1:][i])!=0]) for line in open(f"{self.filefolder}/{file}", "r").read().split("\n") if len(line)!=0]
            else:
                all_features+=[(int(file.split(".")[0]), [i for i in range(len(line.split(" "))) if int(line.split(" ")[i])!=0]) for line in open(f"{self.filefolder}/{file}", "r").read().split("\n") if len(line)!=0]
        all_combs = itertools.combinations(all_features,2)
        for (client1, features1),(client2, features2) in all_combs:
            if client1 not in self.weights:
                self.weights[client1] = dict()
            
            if client2 not in self.weights:
                self.weights[client2] = dict()
            
            _inter = set(features1).intersection(set(features2))        
            _union = set(features1 + features2)
            if len(_union) == 0:
                _union = [1]
            value = len(_inter)/len(_union)
            self.weights[client1][client2] = value
            self.weights[client2][client1] = value


# Initialization
if __name__ == "__main__":
     #   Argument Handling
    params = ["-f", "-o", "-w"]
    if len(set([x for x in params if x in sys.argv[1:]])) not in [2,3]:
        print("Incorrect Parameters\n")
        print("Use: \n-f Folder of Graph Files\n-o output folder(String)\n Optional: -w = use weighted graph")
        sys.exit(-1)
        
    for i in range(len(sys.argv[1:])):
        if sys.argv[i+1] == "-f":
            r_filename = sys.argv[i+2]

        elif sys.argv[i+1] == "-o":
            out = sys.argv[i+2]
    sgp = SGP(r_filename, out)
    if "-w" in sys.argv:
        sgp.load_weights()
        print("Weights Loaded")
    sgp.load_networks_laplacian_matrix()
    print("Created Laplacian Matrix")
    sgp.laplacian_eigendecomposition()
    print("Matrix has been decomposed")
    sgp.get_cluster_number()
    print("Cluster Size Declared")
    sgp.create_clusters()
    print("Clusters Created")
    sgp.load_true_labels()
    sgp.calculate_adj_rand_score()
