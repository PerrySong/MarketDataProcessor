from pathlib import Path
from datetime import date
import json
from io import BytesIO
from zipfile import ZipFile, BadZipFile
import requests

import pandas_datareader.data as web
import pandas as pd

from pprint import pprint

import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from concurrent.futures import ThreadPoolExecutor
import requests

import logging
import boto3
import os
from botocore.exceptions import ClientError

class FundamentalProcessor:

    def __init__(self):
        self.SEC_URL = 'https://www.sec.gov/'
        self.FSN_PATH = 'files/dera/data/financial-statement-and-notes-data-sets/'
        self.AWS_REGION = 'us-west-2'
        self.FUNDAMENTAL_DATA_PATH = 'fundamentals-data'
        self.AWS_PROFILE_NAME = 'gold-miner'

        session = boto3.Session(profile_name=self.AWS_PROFILE_NAME, region_name=self.AWS_REGION)
        self.s3_client = session.client('s3')   # TODO: replace s3_client with this global var

    def create_bucket(self, bucket_name, region=None):
        """Create an S3 bucket in a specified region

        If a region is not specified, the bucket is created in the S3 default
        region (us-east-1).

        :param bucket_name: Bucket to create
        :param region: String region to create bucket in, e.g., 'us-west-2'
        :return: True if bucket created, else False
        """

        # Create bucket
        try:
            if region is None:
                session = boto3.Session(profile_name='gold-miner')
                s3_client = session.client('s3')
                s3_client.create_bucket(Bucket=bucket_name)
            else:
                session = boto3.Session(profile_name=self.AWS_PROFILE_NAME, region_name=self.AWS_REGION)
                s3_client = session.client('s3')
                location = {'LocationConstraint': region}
                s3_client.create_bucket(Bucket=bucket_name,
                                        CreateBucketConfiguration=location)
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def fetch_filing_data(self, filing_periods, data_path):
        for i, (yr, qtr) in enumerate(filing_periods, 1):
            print(f'{yr}-Q{qtr}', end=' ', flush=True)
            filing = f'{yr}q{qtr}_notes.zip'
            path = data_path / f'{yr}_{qtr}' / 'source'
            if not path.exists():
                path.mkdir(exist_ok=True, parents=True)
            url = self.SEC_URL + self.FSN_PATH + filing
            print('url = ' + url)
            # 2020q1 is currently (Oct 2020) in a different location; this may change at some point
            #         if yr == 2020 and qtr == 1:
            #             url = SEC_URL + 'files/node/add/data_distribution/' + filing

            response = requests.get(url).content
            try:
                with ZipFile(BytesIO(response)) as zip_file:
                    for file in zip_file.namelist():
                        local_file = path / file
                        if local_file.exists():
                            continue
                        with local_file.open('wb') as output:
                            for line in zip_file.open(file).readlines():
                                output.write(line)
            except BadZipFile as e:
                print(e)
                print('got bad zip file')
                continue

    def create_bucket(self, bucket_name, region=None):
        """Create an S3 bucket in a specified region

        If a region is not specified, the bucket is created in the S3 default
        region (us-east-1).

        :param bucket_name: Bucket to create
        :param region: String region to create bucket in, e.g., 'us-west-2'
        :return: True if bucket created, else False
        """

        # Create bucket
        try:
            if region is None:
                session = boto3.Session(profile_name=self.AWS_PROFILE_NAME)
                s3_client = session.client('s3')
                s3_client.create_bucket(Bucket=bucket_name)
            else:
                session = boto3.Session(profile_name=self.AWS_PROFILE_NAME, region_name=self.AWS_REGION)
                s3_client = session.client('s3')
                location = {'LocationConstraint': region}
                s3_client.create_bucket(Bucket=bucket_name,
                                        CreateBucketConfiguration=location)
        except ClientError as e:
            logging.error(e)
            return False
        return True

