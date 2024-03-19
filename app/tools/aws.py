from .settings import AWS
from typing import Union
import boto3
import os


class S3:

    def __init__(self):

        self.client = boto3.client(
            's3', 
            aws_access_key_id= AWS["s3"]["access_key_id"], 
            aws_secret_access_key= AWS["s3"]["secret_access_key"]
            )
        
        self.resource = boto3.resource(
            's3', 
            aws_access_key_id= AWS["s3"]["access_key_id"], 
            aws_secret_access_key= AWS["s3"]["secret_access_key"]
        )
        
        self.bucket = AWS["s3"]["bucket_name"]
    

    def download_file(self, s3_path: str = None, local_path: str = None) -> None:
        file_name =  os.path.basename(s3_path)
        local_path = os.path.join(local_path, file_name)
        self.client.download_file(self.bucket, s3_path, local_path)
        print(f"El archivo {file_name} fue descargado en la ruta:\n{os.path.dirname(local_path)}")

        return local_path
    

    def upload_file(self, local_path: str = None, s3_path: str = None) -> None:

        # Sube el archivo al bucket de S3
        file_name = os.path.basename(local_path)
        s3_path = os.path.join(s3_path, file_name)
        self.client.upload_file(local_path, self.bucket, s3_path)
        print(f"El archivo {os.path.basename(local_path)} acaba de ser subido al:\n{os.path.dirname(s3_path)}")
    

    def validate_path(self, s3_path: str = None) -> bool:

        Bucket = self.resource.Bucket(self.bucket)

        for obj in Bucket.objects.filter(Prefix=s3_path):
            if obj.key == s3_path:
                return True
        
        return False


    def search_correct_s3_path(self, s3_paths: list = None, s3_file_name: str = None) -> str:

        paths = [os.path.join(s3_path, s3_file_name) for s3_path in s3_paths]
        valid_paths = [False] * len(s3_paths)

        for i, s3_path in enumerate(paths):
            try:
                self.client.head_object(Bucket=self.bucket, Key=s3_path)
                valid_paths[i] = True
            except:
                pass

        if True in valid_paths:
            index_correct_path = valid_paths.index(True)
            return s3_paths[index_correct_path]
        else:
            return ""


    def get_path_s3(self, file: str, paths: Union[str, list]) -> str:

        if isinstance(paths, list):

            path_files = [self.alidate_path(os.path.join(path, file)) for path in paths]
            if any(path_files):
                return paths[path_files.index(True)]
            
            else:
                return None
        
        elif isinstance(paths, str):

            return paths


class ec2:
    pass