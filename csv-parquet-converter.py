import numpy 
import pandas 
import fastparquet

def lambda_handler(event,context):

	#identifying resource
	s3_object = boto3.client('s3', region_name='us-east-2')

	#access file

	get_file = s3_object.get_object(Bucket='ENTER_BUCKET_NAME_HERE', Key='CSV_FILE_NAME.csv')
	
	get = get_file['Body']

	df = pandas.DataFrame(get)

	#convert csv to parquet function
	def conv_csv_parquet_file(df):
	
		converted_data_parquet = df.to_parquet('converted_data_parquet_version.parquet')
	
	conv_csv_parquet_file(df)

	print("File converted from CSV to parquet completed")

	#uploading the parquet version file

	s3_path = "/converted_to_parquet/" + converted_data_parquet

	put_response = s3_resource.Object('ENTER_BUCKET_NAME_HERE',converted_data_parquet).put(Body=converted_data_parquet)
