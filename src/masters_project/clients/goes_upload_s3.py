import logging
from pathlib import Path

import s3fs
from botocore.exceptions import ClientError, NoCredentialsError

from masters_project.utils import time_track

logger = logging.getLogger(__name__)

# S3 error codes that indicate missing, invalid, or expired credentials.
_CREDENTIAL_ERROR_CODES = {
    "InvalidAccessKeyId",
    "SignatureDoesNotMatch",
    "AccessDenied",
    "ExpiredToken",
    "InvalidToken",
    "AuthorizationHeaderMalformed",
}

_CREDENTIAL_HELP = (
    "AWS credentials are missing or invalid. Configure them in ~/.aws/credentials "
    "(recommended) and select the profile with AWS_PROFILE, or export "
    "AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY in the environment."
)


def _is_credential_error(exc: BaseException) -> bool:
    """Return True if ``exc`` (or any error in its cause chain) is a credential/auth error.

    s3fs often wraps the underlying botocore exception, so the whole
    ``__cause__`` / ``__context__`` chain is inspected.
    """
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, NoCredentialsError):
            return True
        if isinstance(current, ClientError):
            code = current.response.get("Error", {}).get("Code")
            if code in _CREDENTIAL_ERROR_CODES:
                return True
        current = current.__cause__ or current.__context__
    return False


class GoesUploadClient:
    """Client for uploading local GOES CSVs to a destination S3 bucket.

    Wraps s3fs with authenticated access (credentials resolved from the standard
    AWS chain: environment variables, shared config, or an instance/role profile).
    Supports listing existing object keys so uploads can be skipped idempotently.
    """

    def __init__(self, bucket_name: str, anon: bool = False) -> None:
        """Initialize the upload client.

        Args:
            bucket_name: Destination S3 bucket name (without the ``s3://`` prefix).
            anon: If True, use anonymous access; if False (default), use configured
                AWS credentials. Uploads require ``anon=False``.
        """
        self.bucket_name = bucket_name
        self.fs = s3fs.S3FileSystem(anon=anon)
        logger.debug(f"GOES upload client created for bucket '{bucket_name}'")

    def list_existing_keys(self) -> set[str]:
        """List all object keys currently in the bucket.

        Returns:
            Set of object keys (bucket prefix stripped). Empty if the bucket is
            empty or cannot be listed.
        """
        try:
            paths = self.fs.find(self.bucket_name)
        except Exception as e:
            if _is_credential_error(e):
                logger.error(_CREDENTIAL_HELP)
                raise
            logger.exception(
                f"Failed to list existing keys in '{self.bucket_name}'. "
                "Assuming empty (all files will be uploaded)."
            )
            return set()

        prefix = f"{self.bucket_name}/"
        keys = {p[len(prefix) :] for p in paths if p.startswith(prefix)}
        logger.info(f"Found {len(keys)} existing objects in '{self.bucket_name}'.")
        return keys

    @time_track
    def upload(self, local_path: Path, key: str) -> None:
        """Upload a local file to ``key`` within the bucket.

        Args:
            local_path: Path to the local file to upload.
            key: Destination object key within the bucket.

        Raises:
            Exception: Re-raises any error from s3fs during the upload.
        """
        destination = f"{self.bucket_name}/{key}"
        try:
            self.fs.put(str(local_path), destination)
        except Exception as e:
            if _is_credential_error(e):
                logger.error(_CREDENTIAL_HELP)
            else:
                logger.exception(f"Failed to upload {local_path} -> s3://{destination}")
            raise
