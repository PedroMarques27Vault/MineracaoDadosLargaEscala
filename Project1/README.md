# MDLE_Project1

Work done by

Pedro Marques 92926

Rui Filipe 92952



Repository for MDLE Project 1

We weren't able to develop the jupyter notebooks. 

However, the code is well and thoroughly commented

**Link to results https://drive.google.com/drive/folders/1ls2nEejpnE5DFHB3D8VSxOKm2QZL_wQe?usp=sharing**
  
## Exercise 1 - APriori and Assoc Rules

**Scripts**: apriori.py and assoc_rules.py

**Results** 

	CMD: 
	
		apriori.py -f conditions.csv -o apriori_resultsk2.txt -k 2 -t 1000
	
		apriori.py -f conditions.csv -o apriori_resultsk3.txt -k 3 -t 1000
	
		assoc_rules.py -f conditions.csv -o assocrules -k 2 -t 1000
		
	Inside the files aprioriresultsk2.txt and aprioriresultsk3.txt

	Folder with more results available in the link provided above
	
	Folders: subfolders apriorik=2 and apriorik=3  inside folders with the same name for apriori.py and assocrules for assoc_rules.py

**Arguments**

All are mandatory

- -f filename -> filename with the conditions

- -o outputfile -> apriori: filename to output results | assoc_rules: folder to output results

- -k number -> size of the baskets

- -t number -> frequent threshold


**Example of execution** apriori.py -f conditions.csv -o results -k 2 -t 1000



## Exercise 2 - Find Similar Movies By Plot

- We did not complete the last exercise (2.3)
 
**Problem**  Select a combination (of bands and rows) that finds as candidates at least 99.5% of pairs with 80% similarity and less than 5% of pairs with 40% similarity.

*Solution* We found that using b = 27 and r = 3 achieves these results

**Scripts**: movies.py 


**Results** 

	CMD: 
	
		movies.py -mf movie.metadata.tsv -pf plot_summaries.txt -o moviesresults.txt -k 3 -b 27 -r 3 -mv "rocky pink"
		
	Inside the files moviesresults.txt

	Folder with more results available in the link provided above
	
	Folders: moviesresults
	
	

**Arguments**

Mandatory:

- -mf filename -> file with the movies and codes

- -pf filename -> file with plots of the movies

- -o outputfile -> file to output results

- -k number -> number of words in the shingle

- -b number -> number of bands

- -r number -> number of rows

- -mv name -> movie name

*Optional:*

- -lm -> load data from memory, avoiding LSH process. Put simply just get similar movies from previously calculated data

**Example of execution** movies.py -mf movie.metadata.tsv -pf plot_summaries.txt -o results -k 3 -b 27 -r 3 -mv "rocky pink"


