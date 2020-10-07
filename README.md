# aws-csv-to-parquet-converter.py

Title: AWS CSV to Parquet Converter in Python

Cloud Provider: Amazon Web Service (AWS)

Platform: AWS Lambda

Script Language: Python

Script Compatibility: Python 3.6

Dependencies: 

1. Boto3

2. Fast-Parquet or Pyarrow

3. Pandas

4. Numpy


Purpose: 

This Script gets files from Amazon S3 and converts it to Parquet Version for later query jobs and uploads it back to the Amazon S3.


Elements and Explanation:

Python Library Boto3 allows the lambda to get the CSV file from S3 and then Fast-Parquet (or Pyarrow) converts the CSV file into Parquet.


Parameters:

1. Bucket Name and Region

2. CSV File Key Name


Deployment Process:

1. Make a package containing all the dependencies and the given python script.

2. Deploy the package on lambda

3. Execute
