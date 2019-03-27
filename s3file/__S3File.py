import os
import boto3
from io import BytesIO, StringIO


s3 = boto3.resource('s3')


def _split_into_bucket_and_key(path):
    bucket, key = '', ''
    path_split = path.split('/')
    bucket = path_split[0]
    if len(path_split) > 1:
        key = '/'.join(path_split[1:])
    return bucket, key


def s3_xlist(s3_path):
    bucket, key = _split_into_bucket_and_key(s3_path)
    c = s3.meta.client
    p = c.get_paginator('list_objects')
    for page in p.paginate(Bucket=bucket, Prefix=key):
        for content in page['Contents']:
            key = content['Key']
            size = content['Size']
            time = content['LastModified']
            path = '/'.join([bucket, key])
            if size == 0:
                continue
            yield {'path': path, 'size': size, 'time': time}


def s3_stream(s3_path):
    bucket, key = _split_into_bucket_and_key(s3_path)
    res = s3.Object(bucket, key).get()
    return res['Body']


def s3_upload(local_path, s3_path):
    bucket, key = _split_into_bucket_and_key(s3_path)
    c = s3.meta.client
    c.upload_file(local_path, bucket, Key=key)


def s3_download(s3_path, local_path):
    # setup local path
    os.makedirs(os.path.split(local_path)[0], exist_ok=True)
    # do download
    bucket, key = _split_into_bucket_and_key(s3_path)
    b = s3.Bucket(bucket)
    b.download_file(key, local_path)


class S3File:
    def __init__(self, path, mode, tmp='/tmp/s3'):
        self.mode = mode
        self.path = path
        self.tmp = tmp
        self.local = os.path.join(os.path.expanduser(self.tmp), self.path)
        self._fp = None
        # mode assertion
        if not any(self.mode == x for x in ['r', 'rb', 'w', 'wb']):
            raise ValueError('mode should be one of `r`, `rb`, `w` or `wb`')

    def read(self):
        if self.mode == 'r' or self.mode == 'rb':
            return self._fp.read()
        else:
            raise TypeError("this file object is not readable")

    def write(self, content):
        if self.mode == 'w' or self.mode == 'wb':
            return self._fp.write(content)
        else:
            raise TypeError("this file object is not writable")

    def close(self):
        self._fp.close()
        if self.mode == 'w' or self.mode == 'wb':
            s3_upload(self.local, self.path)
            os.remove(self.local)

    def __enter__(self):
        if self.mode == 'r' or self.mode == 'rb':
            self._fp = s3_stream(self.path)
        elif self.mode == 'w' or self.mode == 'wb':
            os.makedirs(os.path.split(self.local)[0], exist_ok=True)
            self._fp = open(self.local, self.mode)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def s3_open(path, mode='rb', cache_dir='/tmp/s3'):
    return S3File(path, mode, cache_dir)


def s3_load(path, mode='rb', force_download=False, cache_dir='~/Downloads'):
    cache_dir = os.path.expanduser(cache_dir)
    path_dir = os.path.split(os.path.join(cache_dir, path))[0]
    os.makedirs(path_dir, exist_ok=True)
    local = os.path.join(cache_dir, path)

    # download the content if needed
    if force_download:
        s3_download(path, local)
    if not os.path.exists(local):
        s3_download(path, local)

    # open and read the content
    if not any(mode == x for x in ['rb', 'r']):
        raise ValueError('mode needs to be `r` or `rb`')
    with open(local, mode) as fp:
        content = fp.read()

    # return StringIO or BytesIO
    if mode == 'r':
        return StringIO(content)
    elif mode == 'rb':
        return BytesIO(content)
    else:
        return None


def s3_save(path, content, cache_dir='~/Downloads'):
    cache_dir = os.path.expanduser(cache_dir)
    path_dir = os.path.split(os.path.join(cache_dir, path))[0]
    os.makedirs(path_dir, exist_ok=True)
    local = os.path.join(cache_dir, path)

    # detect the right `mode` to open the file
    if type(content) == type(StringIO()):
        mode = 'w'
    elif type(content) == type(BytesIO()):
        mode = 'wb'
    else:
        raise ValueError(f"content type {type(content)} is not supported")

    # write the content to the local disk
    with open(local, mode) as fp:
        fp.write(content.getvalue())

    # upload the content
    s3_upload(local, path)
