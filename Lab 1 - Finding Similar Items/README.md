# Lab1-Finding Similar Items
The purpose of this exercise is to find textually similar items based on Jaccard similarity using shingling, minhashing, and locality-sensitive hashing (LSH) techniques and corresponding algorithms.  

The data examined is a collection of BBC-news articles covering five different genres: business, entertainment, politics, tech and sport. The data can be downloaded from the following source: https://www.kaggle.com/pariza/bbc-news-summary.   

To run the code, run the main.py file with three arguments describing the following:  

K (int) - length of k-shingle  
number_of_hash_functions (int) - number of hash fuctions to apply when constructing the singature matrix.  
take_time (boolean) - decides if the codes outputs execution interval  

If no arguments are given to the function, it will use the following as default values:  
K = 4  
number_of_hash_functions = 100  
take_time = False  