"""Private, attempt-scoped S3 transfers for the validator comparison."""
import math
import os
import time
from contextlib import closing
from pathlib import Path


class StorageConfigurationError(ValueError):
    pass


class Store:
    def __init__(self):
        import boto3
        from botocore.config import Config
        from botocore.exceptions import ClientError

        self.bucket = os.getenv("CHSDK_S3_BUCKET")
        credentials = dict(aws_access_key_id=os.getenv("CHSDK_S3_ACCESS_KEY_ID"),
                           aws_secret_access_key=os.getenv("CHSDK_S3_SECRET_ACCESS_KEY"))
        if not self.bucket or not all(credentials.values()):
            raise StorageConfigurationError("missing comparison S3 settings")
        config = Config(signature_version="s3v4", connect_timeout=3, read_timeout=5,
                        retries={"total_max_attempts": 2})
        session = boto3.session.Session()
        self.client = session.client("s3", config=config, **credentials)
        # SigV4 presigns must use the bucket's actual region. S3 includes this
        # header even for object-only credentials without ListBucket permission.
        try:
            response = self.client.head_bucket(Bucket=self.bucket)
        except ClientError as exc:
            response = exc.response
        region = response["ResponseMetadata"]["HTTPHeaders"].get("x-amz-bucket-region")
        if not region:
            self.client.close()
            raise StorageConfigurationError("could not determine comparison bucket region")
        if region != self.client.meta.region_name:
            self.client.close()
            self.client = session.client("s3", config=config, region_name=region, **credentials)

    def sign(self, operation, key, deadline):
        ttl = min(604800, math.ceil(deadline - time.monotonic()))
        if ttl <= 0:
            raise TimeoutError()
        return self.client.generate_presigned_url(operation, Params={"Bucket": self.bucket, "Key": key},
                                                  ExpiresIn=ttl)

    def upload_index(self, key, path):
        with path.open("rb") as data:
            self.client.put_object(Bucket=self.bucket, Key=key, Body=data)

    def delete(self, key):
        self.client.delete_object(Bucket=self.bucket, Key=key)

    def retrieve(self, key, directory, filename, deadline):
        """The thread owns the directory until the last write, even on cancellation."""
        if time.monotonic() >= deadline:
            raise TimeoutError()
        result = self.client.get_object(Bucket=self.bucket, Key=key)
        with closing(result["Body"]) as body, (Path(directory.name) / filename).open("xb") as out:
            for chunk in body.iter_chunks(256 * 1024):
                if time.monotonic() >= deadline:
                    raise TimeoutError()
                out.write(chunk)
        # Same thread and operation as the successful download. Never defer this
        # deletion to comparison, a sibling transfer, or a cancelled async waiter.
        try:
            self.delete(key)
            return True
        except Exception:
            # The pair is still comparable. The attempt retries its pending keys.
            return False
