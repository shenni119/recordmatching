# recordmatching
Fuzzy matching 2 fields from 2 csv's with fuzzywuzzy package

Goal:
Matching records from 2 data sets without any unique or structured identifiers. 

What the application does:
It loops through one dataframe, one record at a time, and applies the fuzzywuzzy package to see if that record fuzzy matches records in another dataframe. You can customize how greedy you want to fuzzy match. It then returns a dataframe with potential matches.

Input:
2 csv's, each with a column you want to fuzzy match to the other csv

Output:
A dataframe/csv, whereby the headers are all the rows values from one csv, and the index is the rows values from the other. The values that matched above the threshold you set will show the fuzzy match value where the index and header intersect. This dataframe is only meant to help you audit the accuracy of the fuzzy matches.
