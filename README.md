# Spark-Assignments

# Spark-Assignments

### Spark Assignment 1 ###

Given two data files transaction.csv, user.csv

#### user file consists of the following fields: ####
  - user_Id
  - EmailId
  - NativeLanguage
  - Location
#### transaction file consists of the following fields: ####
  - Transaction_id
  - product_id
  - userId
  - price
  - product_description
  
userId in both the files denotes the same.

#### Tasks to be performed with above files are as follows: ####
  - 1.Count of unique locations where each product is sold.
  - 2.Find out products bought by each user.
  - 3.Total spending done by each user on each product.
  
#### Code snippet consists of functions as below : ####

  - user_DataFrame : 
  This function reads the user.csv file and data is been stored into a dataframe.
  - transaction_DataFrame : 
  This function reds the transaction.csv file and data is been stored into another dataframe.
  - join_DataFrame : 
  This function joins the two dataframes based on inner join condition with userid column.
  - unique_loc_count : 
  This functions accepts the joined result of two dataframes as an argument and computes the count of unique locations where each product is sold.
  - user_prod : 
  This function accepts the joined result of two dataframes as an argument and computes the products bought by each user.
  -tot_spend : 
  This function accepst the joined result of two dataframes as an argument and computes the total amount of spending done by each user on each product.
  
core directory has two files
  - driver : We call a function from driver and the result expected from that function is seen in the driver 
  - util   : body of the fucntion is described in util
Whenever we call a function from driver, the function is been executed in util and result is been displayed in driver.

resource directory has two files transaction.csv, user.csv in it.

test directory has the test case in it.
