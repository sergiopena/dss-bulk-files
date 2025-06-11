import argparse
import os

from common import process


class EnvDefault(argparse.Action):
    def __init__(self, envvar, required=True, default=None, **kwargs):
        if envvar:
            if envvar in os.environ:
                default = os.environ[envvar]
        if required and default:
            required = False
        super(EnvDefault, self).__init__(default=default, required=required,
                                         **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, values)


parser = argparse.ArgumentParser(prog='dotStatSuite bulk downloader',
                                 description='Download dataflows from dotStatSuite and upload a compressed version and parquet version to S3 Bucket')

parser.add_argument('-d', '--dataflow-url', type=str, action=EnvDefault, envvar='DATAFLOW_URL', required=True,
                    help='The dataflow url to download')
parser.add_argument('-s', '--s3-bucket', type=str, action=EnvDefault, envvar='S3_BUCKET', required=True,
                    help='The S3 bucket to upload the data to')
parser.add_argument('-f', '--filename', type=str, action=EnvDefault, envvar='FILENAME', required=True,
                    help='Output filename')

arg = parser.parse_args()
df_url = arg.dataflow_url.replace("'", "")

process(dataflow_url=df_url, s3_bucket=arg.s3_bucket, filename=arg.filename)
