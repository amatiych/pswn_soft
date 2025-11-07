import boto3
def list_s3_files(bucket_name, prefix=None):
    """
    Lists all files in an S3 bucket (optionally filtered by prefix).
    """
    s3 = boto3.client('s3')
    kwargs = {'Bucket': bucket_name}

    if prefix:
        kwargs['Prefix'] = prefix

    files = []
    while True:
        response = s3.list_objects_v2(**kwargs)
        for obj in response.get('Contents', []):
            files.append(obj['Key'])

        # handle pagination
        if response.get('IsTruncated'):
            kwargs['ContinuationToken'] = response['NextContinuationToken']
        else:
            break

    return files

if __name__ == "__main__":
    bucket = "pswn-test"
    files = list_s3_files(bucket,prefix="portfolios/")
    print(files)