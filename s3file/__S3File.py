import sys
import os
import boto3
from binascii import hexlify

__DEFAULT_CACHE_DIR = '~/Downloads/s3file_cache'
__DEFAULT_TEMP_DIR = '/tmp/s3file'

thismodule = sys.modules[__name__]
s3 = boto3.resource('s3')


def s3_set_profile(profile_name):
    thismodule.s3 = boto3.session.Session(profile_name=profile_name).resource('s3')


def local_xlist(path):
    for root, dirs, files in os.walk(path):
        for name in files:
            yield(os.path.join(root, name))


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


def s3_upload_file(local_path, s3_path):
    bucket, key = _split_into_bucket_and_key(s3_path)
    c = s3.meta.client
    c.upload_file(local_path, bucket, Key=key)


def s3_download_file(s3_path, local_path):
    # setup destination
    os.makedirs(os.path.split(local_path)[0], exist_ok=True)

    # do download
    bucket, key = _split_into_bucket_and_key(s3_path)
    b = s3.Bucket(bucket)
    b.download_file(key, local_path)


def s3_upload(local_path, s3_path):
    # sanitization
    local_path = os.path.abspath(local_path)

    # upload
    if os.path.isfile(local_path):
        s3_upload_file(local_path, s3_path)
    elif os.path.isdir(local_path):
        for path in local_xlist(local_path):
            src_path = path
            rpath = src_path[len(local_path) + 1:]
            dst_path = os.path.join(s3_path, rpath)
            # check conditions
            if not os.path.isfile(src_path):
                continue
            # upload
            s3_upload_file(src_path, dst_path)
    else:
        raise ValueError("Invalid path")


def s3_download(s3_path, local_path):
    # sanitization
    local_path = os.path.abspath(local_path)

    # download
    for files in s3_xlist(s3_path):
        # path conversion
        src_path = files['path']
        rpath = src_path[len(s3_path) + 1:]
        if len(rpath) == 0: # => file
            dst_path = local_path
        else: # => directory
            dst_path = os.path.join(local_path, rpath)
        # download
        s3_download_file(src_path, dst_path)


class S3File:
    def __init__(self, path, mode, tmp):
        self.path = path
        self.mode = mode
        self.tmp = tmp
        self.local = os.path.join(self.tmp, hexlify(self.path.encode()).decode())
        self._fp = None
        # mode assertion
        if not (self.mode in ['r', 'rb', 'w', 'wb']):
            raise ValueError('mode should be one of `r`, `rb`, `w` or `wb`')

    def read(self, n=None):
        # read n bytes into b
        if self.mode == 'r' or self.mode == 'rb':
            if n is None:
                b = self._fp.read()
            else:
                b = self._fp.read(n)
        else:
            raise TypeError("this file object is not readable")

        # b => s if needed
        if self.mode == 'r':
            return b.decode()
        elif self.mode == 'rb':
            return b
        else:
            return None

    def write(self, content):
        if self.mode == 'wb' and isinstance(content, bytes):
            return self._fp.write(content)
        elif self.mode == 'w' and isinstance(content, str):
            return self._fp.write(content)
        else:
            if self.mode in ('w', 'wb'):
                raise TypeError("the content and mode mismatch")
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


def s3_open(path, mode='rb', cache_dir=__DEFAULT_TEMP_DIR):
    return S3File(path, mode, cache_dir)


def s3_load(path, mode='rb', force_download=False, cache_dir=__DEFAULT_CACHE_DIR):
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
    if not mode in ['rb', 'r']:
        raise ValueError('mode needs to be either `r` or `rb`')
    with open(local, mode) as fp:
        content = fp.read()

    return content


def s3_save(path, content, cache_dir=__DEFAULT_CACHE_DIR):
    cache_dir = os.path.expanduser(cache_dir)
    path_dir = os.path.split(os.path.join(cache_dir, path))[0]
    os.makedirs(path_dir, exist_ok=True)
    local = os.path.join(cache_dir, path)

    # detect the right `mode` to open the file
    if isinstance(content, str):
        mode = 'w'
    elif isinstance(content, bytes):
        mode = 'wb'
    else:
        raise ValueError(f"the content type {type(content)} is not supported")

    # write the content to the local disk
    with open(local, mode) as fp:
        fp.write(content)

    # upload the content
    s3_upload(local, path)
