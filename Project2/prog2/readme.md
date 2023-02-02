## Program 2: Collaborative Filtering Item-Item Approach  
**Algorithm**: collab_filtering.py  
> **How To Run**  
- -rf Filename_of_User_Ratings  
>> Filename with user ratings  
- -o recommendation_output_folder  
>> Folder Inside CFItemItem where results are stored   
- -t similarity_threshold  
>> Pairs of movies with similarity equal or below similarity_threshold are not considered similar  

**Optional Arguments**: 
- --partition-size size_of_partition
>> Number of similarity pairs records per partitions. These are saved inside CFItemItem/similarities  
- -lm  
>> Loads similarity records from memory, instead of calculating all of them again  