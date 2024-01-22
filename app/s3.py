from dotenv import load_dotenv
import boto3
import os

load_dotenv()

# Crear una instancia del cliente de S3
client = boto3.client(
    's3', 
    aws_access_key_id= os.environ.get("AWS_ACCESS_KEY_ID"), 
    aws_secret_access_key= os.environ.get("AWS_SECRET_ACCESS_KEY")
)
# nombre del bucket donde vamos a sacar la informacion
bucket = os.environ.get("AWS_BUCKET_NAME")