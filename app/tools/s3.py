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

resource = boto3.resource(
    's3', 
    aws_access_key_id= os.environ.get("AWS_ACCESS_KEY_ID"), 
    aws_secret_access_key= os.environ.get("AWS_SECRET_ACCESS_KEY")
)

# nombre del bucket donde vamos a sacar la informacion
bucket = os.environ.get("AWS_BUCKET_NAME")

def download_file(s3_path: str = None, local_path: str = None, file_name: str = None) -> None:
    local_path = os.path.join(local_path, file_name)
    client.download_file(bucket, s3_path, local_path)
    print(f"El archivo {file_name} fue descargado en la ruta:\n{os.path.dirname(local_path)}")

def upload_file(local_path: str = None, s3_path: str = None) -> None:

    # Sube el archivo al bucket de S3
    file_name = os.path.basename(local_path)
    s3_path = os.path.join(s3_path, file_name)
    client.upload_file(local_path, bucket, s3_path)
    print(f"El archivo {os.path.basename(local_path)} acaba de ser subido al:\n{os.path.dirname(s3_path)}")


def validate_path(s3_path: str = None) -> bool:

    Bucket = resource.Bucket(bucket)

    for obj in Bucket.objects.filter(Prefix=s3_path):
        if obj.key == s3_path:
            return True
    
    return False


def search_correct_s3_path(s3_paths: list = None, s3_file_name: str = None) -> str:

    paths = [os.path.join(s3_path, s3_file_name) for s3_path in s3_paths]
    valid_paths = [False] * len(s3_paths)

    for i, s3_path in enumerate(paths):
        try:
            client.head_object(Bucket=bucket, Key=s3_path)
            valid_paths[i] = True
        except:
            pass

    if True in valid_paths:
        index_correct_path = valid_paths.index(True)
        return s3_paths[index_correct_path]
    else:
        return ""