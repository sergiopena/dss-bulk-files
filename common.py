import asyncio
import os
from pathlib import Path
from tempfile import TemporaryDirectory

import boto3
import httpx
from botocore.exceptions import ClientError


def process(dataflow_url: str, s3_bucket: str, filename: str):
    with TemporaryDirectory() as tmpdirname:
        asyncio.run(download_file(dataflow_url, Path(f"{tmpdirname}/csv")))
        compress_file(Path(f"{tmpdirname}/csv"), output_filename=filename)
        convert_to_parquet(Path(f"{tmpdirname}/csv"), output_filename=filename)
        upload_file(f"{filename}.zip", s3_bucket, object_name=f"{filename}.zip")
        upload_file(f"{filename}.parquet", s3_bucket, object_name=f"{filename}.parquet")


async def download_file(url: str, destination: Path):
    print('Starting download...')
    async with httpx.AsyncClient(timeout=30) as client:
        async with client.stream("GET", url) as response:
            response.raise_for_status()

            # Check if content is Brotli-compressed
            if 'content-encoding' in response.headers:
                print(f"Content encoding: {response.headers['content-encoding']}")

            with open(destination, "wb") as file:
                async for chunk in response.aiter_bytes():
                    file.write(chunk)
    print(f'File downloaded to {destination}')


def compress_file(path: Path, output_filename: str = 'compressed_file.zip'):
    print('Compressing file...')
    from zipfile import ZipFile
    with ZipFile(f'{output_filename}.zip', 'w') as zipf:
        zipf.write(path)
    print(f'File compressed to {output_filename}.zip')


def convert_to_parquet(path: Path, output_filename: str = 'output.parquet'):
    print('Converting CSV to Parquet...')
    import pandas as pd
    df = pd.read_csv(path)
    df.to_parquet(f'{output_filename}.parquet', engine='pyarrow', compression='snappy')
    print(f'File converted to {output_filename}.parquet')


def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    print(f'Uploading {file_name} to bucket {bucket}...')
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        print(e)
        return False
    print(f'File {file_name} uploaded to bucket {bucket} as {object_name}')
    return True
