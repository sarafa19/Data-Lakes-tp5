import boto3

def list_s3_files(bucket_name):
    """Affiche les fichiers présents dans un bucket S3 (LocalStack)."""
    s3 = boto3.client('s3',
        endpoint_url='http://localhost:4566',
        aws_access_key_id='test',
        aws_secret_access_key='test',
        region_name='us-east-1'
    )

    try:
        response = s3.list_objects_v2(Bucket=bucket_name)
        contents = response.get("Contents")

        if contents:
            print(f" Fichiers dans le bucket '{bucket_name}':")
            for obj in contents:
                print(f" - {obj['Key']}")
        else:
            print(f" Le bucket '{bucket_name}' est vide.")
    except Exception as e:
        print(f" Erreur lors de l'accès au bucket : {e}")


list_s3_files("raw")