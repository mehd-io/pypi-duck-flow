from pydantic import BaseModel, Field
from typing import List, Union, Annotated, Type
from pydantic import BaseModel, ValidationError
from datetime import datetime
from typing import Optional
import pandas as pd

DUCKDB_EXTENSION = ["aws", "httpfs"]


class File(BaseModel):
    filename: Optional[str]
    project: Optional[str]
    version: Optional[str]
    type: Optional[str]


class Libc(BaseModel):
    lib: Optional[str]
    version: Optional[str]


class Distro(BaseModel):
    name: Optional[str]
    version: Optional[str]
    id: Optional[str]
    libc: Optional[Libc]


class Implementation(BaseModel):
    name: Optional[str]
    version: Optional[str]


class Installer(BaseModel):
    name: Optional[str]
    version: Optional[str]


class System(BaseModel):
    name: Optional[str]
    release: Optional[str]


class Details(BaseModel):
    installer: Optional[Installer]
    python: Optional[str]
    implementation: Optional[Implementation]
    distro: Optional[Distro]
    system: Optional[System]
    cpu: Optional[str]
    openssl_version: Optional[str]
    setuptools_version: Optional[str]
    rustc_version: Optional[str]


class FileDownloads(BaseModel):
    timestamp: Optional[datetime]
    country_code: Optional[str]
    url: Optional[str]
    project: Optional[str]
    file: Optional[File]
    details: Optional[Details]
    tls_protocol: Optional[str]
    tls_cipher: Optional[str]


class PypiJobParameters(BaseModel):
    start_date: str = "2019-04-01"
    end_date: str = "2023-11-30"
    pypi_project: str = "duckdb"
    table_name: str
    gcp_project: str
    timestamp_column: str = "timestamp"
    destination: Annotated[
        Union[List[str], str], Field(default=["local"])
    ]  # local, s3, md
    s3_path: Optional[str]
    aws_profile: Optional[str]


class DataFrameValidationError(Exception):
    """Custom exception for DataFrame validation errors."""


def validate_dataframe(df: pd.DataFrame, model: Type[BaseModel]):
    """
    Validates each row of a DataFrame against a Pydantic model.
    Raises DataFrameValidationError if any row fails validation.

    :param df: DataFrame to validate.
    :param model: Pydantic model to validate against.
    :raises: DataFrameValidationError
    """
    errors = []

    for i, row in enumerate(df.to_dict(orient="records")):
        try:
            model(**row)
        except ValidationError as e:
            errors.append(f"Row {i} failed validation: {e}")

    if errors:
        error_message = "\n".join(errors)
        raise DataFrameValidationError(
            f"DataFrame validation failed with the following errors:\n{error_message}"
        )
