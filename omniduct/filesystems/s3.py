import logging
from omniduct.filesystems.base import FileSystemClient, FileSystemFileDesc

# Python 2 compatibility imports
try:
    FileNotFoundError
except NameError:
    FileNotFoundError = IOError


class S3Client(FileSystemClient):
    """
    This Duct connects to an Amazon S3 bucket instance using the `boto3`
    library. Authentication is (optionally) handled using `opinel`.

    Attributes:
        bucket (str): The name of the Amazon S3 bucket to use.
        aws_profile (str): The name of configured AWS profile to use. This should
            refer to the name of a profile configured in, for example,
            `~/.aws/credentials`. Authentication is handled by the `opinel`
            library, which is also aware of environment variables.
    """

    PROTOCOLS = ['s3']
    DEFAULT_PORT = 80

    def _init(self, bucket=None, aws_profile=None, path_separator='/',
              use_opinel=False, skip_hadoop_artifacts=True):
        """
        bucket (str): The name of the Amazon S3 bucket to use.
        aws_profile (str): The name of configured AWS profile to use. This should
            refer to the name of a profile configured in, for example,
            `~/.aws/credentials`. Authentication is handled by the `opinel`
            library, which is also aware of environment variables.
        path_separator (str): Amazon S3 is essentially a key-based storage
            system, and so one is free to choose an arbitrary "directory"
            separator. This defaults to '/' for consistency with other
            filesystems.
        use_opinel (bool): Use Opinel to extract AWS credentials. This is mainly
            useful if you have used opinel to set up MFA. Note: Opinel must be
            installed manually alongside omniduct to take advantage of this
            feature.
        skip_hadoop_artifacts (bool): Whether to skip hadoop artifacts like
            '*_$folder$' when enumerating directories (default=True).

        Note: aws_profile, if specified, should be the name of a profile as
        specified in ~/.aws/credentials. Authentication is handled by the
        `opinel` library, which is also aware of environment variables.
        Set up your command line aws client, and if it works, this should too.
        """
        assert bucket is not None, 'S3 Bucket must be specified using the `bucket` kwarg.'
        self.bucket = bucket
        self.aws_profile = aws_profile
        self.use_opinel = use_opinel
        self.skip_hadoop_artifacts = skip_hadoop_artifacts
        self.__path_separator = path_separator
        self._client = None

        # Ensure self.host is updated with correct AWS region
        import boto3
        self.host = 'autoscaling.{}.amazonaws.com'.format(
            boto3.Session(profile_name=self.aws_profile).region_name
        )

        # Mask logging from botocore's vendored libraries
        logging.getLogger('botocore.vendored').setLevel(100)

    def _connect(self):
        import boto3

        if self.use_opinel:
            from opinel.utils.credentials import read_creds

            # Refresh access token, and attach credentials to current object for debugging
            self._credentials = read_creds(self.aws_profile)

            session = boto3.Session(
                aws_access_key_id=self._credentials['AccessKeyId'],
                aws_secret_access_key=self._credentials['SecretAccessKey'],
                aws_session_token=self._credentials['SessionToken'],
                profile_name=self.aws_profile,
            )
        else:
            session = boto3.Session(profile_name=self.aws_profile)

        self._session = session
        self._client = session.client('s3')
        self._resource = session.resource('s3')

    def _is_connected(self):
        if self._client is None:
            return False
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
        return self.path_separator

    def _path_separator(self):
        return self.__path_separator

    def _path(self, path):
        path = super(S3Client, self)._path(path)
        if path.startswith(self.path_separator):
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
        if path.endswith(self.path_separator):
            path = path[:-len(self.path_separator)]
        paginator = self._client.get_paginator('list_objects')
        iterator = paginator.paginate(
            Bucket=self.bucket,
            Prefix=path + (self.path_separator if path else ''),
            Delimiter=self.path_separator,
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
                    name=prefix['Prefix'][:-1].split(self.path_separator)[-1],  # Remove trailing slash
                    type='directory',
                )
            for prefix in response_data.get('Contents', []):
                if self.skip_hadoop_artifacts and prefix['Key'].endswith('_$folder$'):
                    continue
                yield FileSystemFileDesc(
                    fs=self,
                    path=prefix['Key'],
                    name=prefix['Key'].split(self.path_separator)[-1],
                    type='file',
                    bytes=prefix['Size'],
                    owner=prefix['Owner']['DisplayName'] if 'Owner' in prefix else None,
                    last_modified=prefix['LastModified']
                )

    # TODO: Interestingly, directly using Amazon S3 methods seems slower than generic approach. Hypothesis: keys is not asynchronous.
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

    def _mkdir(self, path, recursive, exist_ok):
        if not path.endswith('/'):
            path += '/'
        if not self._exists(path):
            self._client.put_object(Bucket=self.bucket, Key=path)

    def _remove(self, path, recursive):
        if recursive:
            bucket = self._resource.Bucket(self.bucket)
            to_delete = []
            for obj in bucket.objects.filter(Prefix=path):
                to_delete.append({'Key': obj.key})
                if len(to_delete) == 1000:  # Maximum number of simultaneous deletes is 1000
                    self._client.delete_objects(Bucket=self.bucket, Delete={'Objects': to_delete})
                    to_delete = []
            self._client.delete_objects(Bucket=self.bucket, Delete={'Objects': to_delete})
        else:
            self._client.delete_object(Bucket=self.bucket, Key=path)

    # File handling

    def _file_read_(self, path, size=-1, offset=0, binary=False):
        if not self.isfile(path):
            raise FileNotFoundError("File `{}` does not exist.".format(path))

        obj = self._resource.Object(self.bucket, path)
        body = obj.get()['Body'].read()

        if not binary:
            body = body.decode('utf-8')
        if offset > 0:
            body = body[offset:]
        if size >= 0:
            body = body[:size]
        return body

    def _file_append_(self, path, s, binary):
        raise NotImplementedError("Support for S3 append operation has yet to be implemented.")

    def _file_write_(self, path, s, binary):
        obj = self._resource.Object(self.bucket, path)
        if not binary:
            s = s.encode('utf-8')
        obj.put(Body=s)
        return True
