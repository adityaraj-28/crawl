import pandas as pd
import boto3
import gzip
from warcio.archiveiterator import ArchiveIterator
import io
from bs4 import BeautifulSoup
import re


def run():
    df = pd.read_csv('input.csv')
    for index, row in df.iterrows():
        index = index + 3
        url = row['url']
        key = row['warc_filename']
        offset = row['warc_record_offset']
        length = row['warc_record_length']
        response = s3.get_object(Bucket=BUCKET_NAME, Key=key, Range=f'bytes={offset}-{offset + length - 1}')
        decompressed_body = gzip.decompress(response['Body'].read())
        warc_data = io.BytesIO(decompressed_body)

        # iterate over the WARC records in the file
        for record in ArchiveIterator(warc_data):
            # check if the record is a response record
            if record.rec_type == 'response':
                # extract the HTTP status code and response body
                http_headers = record.http_headers
                status_code = http_headers.get_statuscode()
                response_body = record.content_stream().read()
                html_content = response_body.decode('unicode_escape')
                file = open(f'apple_{index}.html', 'w+')
                file.write(html_content)
                body_text = extract_text(html_content)
                file_text = open(f'apple_{index}.txt', 'w+')
                file_text.write(body_text)
        break


def extract_text(html_content):
    soup = BeautifulSoup(html_content, 'lxml')
    body_text = soup.body.get_text(separator='\n')
    body_text = re.sub(r'\n\s*\n', '\n', body_text)
    return body_text


if __name__ == '__main__':
    BUCKET_NAME = 'commoncrawl'
    s3 = boto3.client('s3')
    run()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
