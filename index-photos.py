import json
import urllib.parse
import boto3
from requests_aws4auth import AWS4Auth
import urllib3
from opensearchpy import OpenSearch, RequestsHttpConnection
import base64
from datauri import DataURI

http = urllib3.PoolManager()

s3 = boto3.client('s3')
rek = boto3.client('rekognition')
opensearch = boto3.client('opensearch')

region = 'us-east-1'
host = 'search-photos-cad4x5ywxrwuht7vhvml7yom4e.us-east-1.es.amazonaws.com'

def lambda_handler(event, context):
    # Get the object from the event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    time = event['Records'][0]['eventTime']
    

    labels = []
    
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        content_type = response['ContentType']
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
        
    try:
        response2 = s3.get_object(Bucket=bucket, Key=key)
        print(response2)
        meta_data = response2['ResponseMetadata']['HTTPHeaders']['x-amz-meta-customlabels']
        custom_labels = list(meta_data.split(",")) 
        for label in custom_labels:
            labels.append(label.strip().lower())
    except Exception as e:
        print(e)
    
    try:
        response3 = s3.get_object(Bucket=bucket, Key=key)
        picture64 = response3['Body'].read().decode('utf-8')
        print(picture64)
        #picture = base64.b64decode(picture64 + b'==')
        #picture_data = picture64.decode('utf-8')
        #picture_data = base64.b64decode(bytes(picture64,'utf-8'), validate=True)
        picture_data = DataURI(picture64)
        picture_data = picture_data.data
        
        detect_response = rek.detect_labels(Image = {'Bytes': picture_data}, MaxLabels= 5, Features=['GENERAL_LABELS'])
        for label in detect_response['Labels']:
            labels.append(label['Name'].lower())
    except Exception as e:
        print(e)
        raise(e)
    
    json_string = '{"objectKey": "' + str(key) + '", "bucket": "' + str(bucket) + '", "createdTimeStamp": "' + str(time) + '", "labels": ' + str(labels) + '}'
    print(json_string)
    try:
        # Build the OpenSearch client
        client = OpenSearch(
            hosts=[{'host': host, 'port': 443}],
            http_auth=get_awsauth(region, 'es'),
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection
        )
        # It can take up to a minute for data access rules to be enforced
        #time.sleep(45)

        # Create index
        #response3 = client.indices.create('photos')
        #print('\nCreating index:')
        #print(response3)
    
        # Add a document to the index.
        response4 = client.index(
            index='photos',
            body={
                'objectKey': str(key),
                'bucket': str(bucket),
                'createdTimeStamp': str(time),
                'lables': str(labels)
                },
            id=str(key),
        )
        print('add picture')
        print(response4)
    except Exception as e:
        print(e)
        raise(e)
    return

#def detect_labels(bucket, key):
 #   response = rek.detect_labels(
  #  Image={
   #     'S3Object': {
    #        'Bucket': bucket,
     #       'Name': key
      #  }},
    #MaxLabels=5,
    #Features=[
    #    'GENERAL_LABELS'
    #])
    #return response
    
def get_awsauth(region, service):
    cred = boto3.Session().get_credentials()
    return AWS4Auth(cred.access_key,
                    cred.secret_key,
                    region,
                    service,
                    session_token=cred.token)
