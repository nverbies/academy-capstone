import boto3


my_bucket = 's3://dataminded-academy-capstone-resources/raw/open_aq/'
my_bucket = 'dataminded-academy-capstone-resources'

s3_client = boto3.resource("s3")
bucket = s3_client.Bucket(my_bucket)
for object in bucket.objects.all():
    print(object)