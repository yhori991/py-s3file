## s3file
A simple utility tool to access s3 object through basic File I/O operations.

## Dependencies

- boto3

## Installation
To install s3file, simply use pipenv:

```bash
pipenv install py-s3file
```

## Usage

__Basic File I/O operations__

Open/Close:

```python
import s3file

fp = s3file.open(path='s3_bucket_name/object_key', mode='r')
content = fp.read()
fp.close()
```

Read:

```python
import s3file

with s3file.open(path='s3_bucket_name/object_key', mode='r') as fp:
    content = fp.read()
```

Write:

```python
import s3file

with s3file.open(path='s3_bucket_name/object_key', mode='w') as fp:
    fp.write('some_string_object')
```

__Utility functions__

The functions below automatically cache the content on local disk (default: on `~/Downloads/s3file_cache`).
This allows quick access to the same content for the subsequent function calls.

Load:

```python
import s3file

content = s3file.load(path='path_to_the_s3_object')
```

Save:

```python
import s3file

s3file.save(path='path_to_the_s3_object', content='content_to_write')
```

## License
MIT
