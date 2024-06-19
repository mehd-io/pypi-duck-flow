from pydantic import BaseModel, Field
from typing import List, Union, Annotated, Type
from pydantic import BaseModel, ValidationError
from datetime import datetime
from typing import Optional, Dict
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
    ci: Optional[bool]


class FileDownloads(BaseModel):
    timestamp: Optional[datetime]=None
    country_code: Optional[str]=None
    url: Optional[str]=None
    project: Optional[str]=None
    file: Optional[File]=None
    details: Optional[Details]=None
    tls_protocol: Optional[str]=None
    tls_cipher: Optional[str]=None

    @property
    def pandas_dtypes(self) -> Dict[str, str]:
        return {
            "timestamp": "datetime64[ns, UTC]",
            "country_code": "object", 
            "url": "object",
            "project": "object",
            "file": "object", 
            "details": "object", 
            "tls_protocol": "object",
            "tls_cipher": "object",
        }


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


def duckdb_ddl_file_downloads(table_name="pypi_file_downloads"):
    return f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        timestamp TIMESTAMP WITH TIME ZONE,
        country_code VARCHAR,
        url VARCHAR,
        project VARCHAR,
        file STRUCT("filename" VARCHAR, "project" VARCHAR, "version" VARCHAR, "type" VARCHAR),
        details STRUCT("installer" STRUCT("name" VARCHAR, "version" VARCHAR), "python" VARCHAR, "implementation" STRUCT("name" VARCHAR, "version" VARCHAR), "distro" STRUCT("name" VARCHAR, "version" VARCHAR, "id" VARCHAR, "libc" STRUCT("lib" VARCHAR, "version" VARCHAR)), "system" STRUCT("name" VARCHAR, "release" VARCHAR), "cpu" VARCHAR, "openssl_version" VARCHAR, "setuptools_version" VARCHAR, "rustc_version" VARCHAR, "ci" BOOLEAN),
        tls_protocol VARCHAR,
        tls_cipher VARCHAR
    )
    """
