import re

from .base import FileSystemClient, FileSystemFileDesc
from ..utils.debug import logger


class S3Client(FileSystemClient):

    PROTOCOLS = ['s3']
    DEFAULT_PORT = 80

    def _init(self, bucket=None, aws_profile='default'):
        # Note: aws_profile, if specified, should be the name of a profile as
        # specified in ~/.aws/credentials. Authentication is handled by the
        # `opinel` library, which is also aware of environment variables.
        # Set up your command line aws client, and if it works, this should too.
        assert bucket is not None, 'S3 Bucket must be specified using the `bucket` kwarg.'
        self.bucket = bucket
        self.aws_profile = aws_profile

        self._setup_session()  # Ensure self.host is updated with correct AWS region

    def _setup_session(self):
        import boto3
        s = boto3.Session(profile_name=self.aws_profile)
        self._session = s
        self._client = s.client('s3')
        self._resource = s.resource('s3')
        self.host = 'autoscaling.{}.amazonaws.com'.format(self._client.meta.region_name)

    def _connect(self):
        from opinel.utils.credentials import read_creds

        # Refresh access token, and attach credentials to current object for debugging
        self._credentials = read_creds(self.aws_profile)

        # Update AWS session, client and resource objects
        self._setup_session()

    def _is_connected(self):
        # Check if still able to perform requests against AWS
        import botocore
        try:
            self._client.list_buckets()
        except botocore.exceptions.ClientError as e:
            if len(e.args) > 0:
                if 'ExpiredToken' in e.args[0] or 'InvalidToken' in e.args[0]:
                    return False
                elif 'AccessDenied' in e.args[0]:
                    return True

    def _disconnect(self):
        pass

    # Path properties and helpers

    def _path_home(self):
        return '/'

    def _path_separator(self):
        return '/'

    def _path(self, path):
        path = super(S3Client, self)._path(path)
        if path.startswith('/'):
            path = path[1:]
        return path

    # File node properties

    def _exists(self, path):
        return self.isfile(path) or self.isdir(path)

    def _isdir(self, path):
        response = next(iter(self.__dir_paginator(path)))
        if 'CommonPrefixes' in response or 'Contents' in response:
            return True
        return False

    def _isfile(self, path):
        try:
            self._client.get_object(Bucket=self.bucket, Key=path or '')
            return True
        except:
            return False

    # Directory handling and enumeration

    def __dir_paginator(self, path):
        if path.endswith('/'):
            path = path[:-1]
        paginator = self._client.get_paginator('list_objects')
        iterator = paginator.paginate(
            Bucket=self.bucket,
            Prefix=path + ('/' if path else ''),
            Delimiter='/',
            PaginationConfig={'PageSize': 500}
        )
        return iterator

    def _dir(self, path):
        iterator = self.__dir_paginator(path)

        for response_data in iterator:
            for prefix in response_data.get('CommonPrefixes', []):
                yield FileSystemFileDesc(
                    fs=self,
                    path=prefix['Prefix'][:-1],
                    name=prefix['Prefix'][:-1].split('/')[-1],  # Remove trailing slash
                    type='directory',
                )
            for prefix in response_data.get('Contents', []):
                yield FileSystemFileDesc(
                    fs=self,
                    path=prefix['Key'],
                    name=prefix['Key'].split('/')[-1],
                    type='file',
                    bytes=prefix['Size'],
                    owner=prefix['Owner']['DisplayName'],
                    last_modified=prefix['LastModified']
                )

    # TODO: Interestingly, directly using Amazon S3 methods seems slower than generic approach. Hypothesis: keys is not async.
    # def _find(self, path_prefix, **attrs):
    #     if len(set(attrs).difference(('name',))) > 0 or hasattr(attrs.get('name'), '__call__'):
    #         logger.warning('Falling back to recursive search, rather than using S3, since find requires filters on more than just name.')
    #         for result in super(S3Client, self)._find(path_prefix, **attrs):
    #             yield result
    #
    #     pattern = re.compile(attrs.get('name') or '.*')
    #
    #     b = self._resource.Bucket(self.bucket)
    #     keys = b.objects.filter(Prefix=path_prefix)
    #     for k in keys:
    #         if pattern is None or pattern.match(k.key[len(path_prefix):]):
    #             yield k.key

    def _mkdir(self, path, recursive):
        raise NotImplementedError("Support for S3 write operations has yet to be implemented.")

    # File handling

    def _file_read_(self, path, size=-1, offset=0, binary=False):
        assert self.isfile(path), "File `{}` does not exist.".format(path)
        obj = self._resource.Object(self.bucket, path)
        body = obj.get()['Body'].read()

        if not binary:
            body = body.decode()

        return body

    def _file_append_(self, path, s, binary):
        raise NotImplementedError("Support for S3 write operations has yet to be implemented.")

    def _file_write_(self, path, s, binary):
        raise NotImplementedError("Support for S3 write operations has yet to be implemented.")
