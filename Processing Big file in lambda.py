import json
import boto3
import zipfile
from io import BytesIO

def lambda_handler(event, context):
    # TODO implement
    s3_resource = boto3.resource('s3')
    client = boto3.client('s3')
    zip_file = "final.zip"
    folder = zip_file.split(".")[0]
    zip_obj = s3_resource.Object(bucket_name="s3-source-bucket", key=zip_file)
    chunk_size = 200000000
    buffer = BytesIO(zip_obj.get()["Body"].read())
    zipObj = zipfile.ZipFile(buffer)
    del buffer
    listOfiles = zipObj.namelist()
    for elem in listOfiles:
        file_size = zipObj.getinfo(elem).file_size
        response = client.create_multipart_upload(Bucket='s3-final-bucket', Key=f'{folder}/{elem}')
        uploadid = response['UploadId']
        file_in = zipObj.open(elem)
        part_number = 1
        etag_part_number = []
        while True:
            #read based on chunk size
            data = file_in.read(chunk_size)
            if data:
                cur_size = file_in.tell()
                if file_size==cur_size:
                    #remove last line
                    print("last chunk...", cur_size)
                    data = data.split(b"\n")[:-1]
                    data = b"\n".join(data)
                response = client.upload_part(Body= data, Bucket='s3-final-bucket', Key=f'{folder}/{elem}', UploadId=uploadid, PartNumber=part_number)
                etag = response['ETag']
                etag_part_number.append({'ETag': etag, 'PartNumber':part_number})
                part_number = part_number + 1
            else:
                print('data splitted')
                m_upload = {'Parts': etag_part_number}
                client.complete_multipart_upload(Bucket='s3-final-bucket', Key=f'{folder}/{elem}', MultipartUpload=m_upload, UploadId=uploadid)
                break
