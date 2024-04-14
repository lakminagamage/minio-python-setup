from minio import Minio
from minio.error import S3Error
import os

# create a minio client with endpoint, access key and secret key
client = Minio(
    os.getenv("SERVER_URL"),
    access_key=os.getenv("API_KEY"),
    secret_key=os.getenv("API_SECRET"),
    secure=False #True or flase as per your server using TLS or not.
    
)

# Function to upload files to MinIO server
def test_upload():
    try:
        if not client.bucket_exists("verifica"):
            client.make_bucket("verifica")
            print("Bucket created successfully.")
        else:
            print("Bucket already exists.")

        files = os.listdir("./fingerprints")

        for i in files:
            client.fput_object("verifica", i, f"./fingerprints/{i}")
            print(f"Uploaded {i} to MinIO server successfully.")

    except S3Error as e:
        print(f"Error: {e}")


# Function to get file by name from MinIO server
def get_file_by_name(file_name):
    try:
        client.fget_object("verifica", file_name, f"./downloads/{file_name}")
        
        print(f"Downloaded {file_name} from MinIO server successfully.")
    except S3Error as e:
        print(f"Error: {e}")


# Function to download all files from MinIO server
def download_all_files():
    try:
        objects = client.list_objects("verifica", recursive=True)
        for obj in objects:
            print(obj.object_name)
            client.fget_object("verifica", obj.object_name, f"./downloads/{obj.object_name}")
            print(f"Downloaded {obj.object_name} from MinIO server successfully.")
    except S3Error as e:
        print(f"Error: {e}")


test_upload()
get_file_by_name("AG_7105_R_001.tif")
download_all_files()




