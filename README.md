# Exploratory-Data-Analysis - EDA
Spark library for doing exploratory data analysis on your data set in a scalable way.


### The problem

Before running any algorithm on a data set it is essential to understand how the data looks like and what are the expected edge cases.
Maybe you have some expectations about the distribution of the numbers. Are all the buckets that you expect to find in the dataset populated with numbers?
Very often a data set is provided which contains two types of columns:

- dimensions

- measurements

Dimensions are columns that help describe the data points such as: Date, UniqueID, Gender, Postcode, Country, Categories.
Measurements are metrics that quantitatively characterize the described datum.

Depending on your analytical task, some sort of ordering or bucketing will be used in your model. Some examples:
- process daily transactions of a customer

- aggregate daily transactions of a merchant

- count the number of unique customers per merchant or postcode, sector or district

- number of transactions per day.

Let's take as example the metric : number of daily transactions per merchant

This is made out of two concepts. A key which is (dayId, merchantId) and a value count(transactions).
Depending on the richness of your dataset is there may be keys with high number of transactiona and keys with very small number. This becomes a problem especially when you are dealing with a sample dataset which promises to capture the entire diversity of the original, complete dataset.

###the library

For a collection / RDD of type T, given a :

- function that maps an element of T to a *key*

- function that maps an element of T to a *value* that corresponds to the *key*

- function to aggregate in a monoid-al fashion two *values*
 
