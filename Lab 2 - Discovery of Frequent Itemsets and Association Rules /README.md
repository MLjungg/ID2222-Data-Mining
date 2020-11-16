# Lab1-Discovery of Frequent Itemsets and Assocation Rules
The purpose of this exercise is to find frequent itemsets with corresponding assocations rules.


The data examined is a collection of 100.000 sale transactions with approximately 10 items per transactions.

To run the code, run the main.py file with two arguments describing the following:  

S (int) - similarity threshold between two items (how many times they need to appear)  
Confidence (int) - The confidence threshold for the assoication rule.

If no arguments are given to the function, it will use the following as default values:  
S = 1000  
number_of_hash_functions = 0.9  
