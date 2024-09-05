from typing import Type
import pyarrow as pa
from pydantic import BaseModel, Field, ValidationError
from datetime import datetime
from typing import Optional, Union, List, Annotated
import hashlib
import json


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
    timestamp: Optional[datetime] = None
    country_code: Optional[str] = None
    url: Optional[str] = None
    project: Optional[str] = None
    file: Optional[File] = None
    details: Optional[Details] = None
    tls_protocol: Optional[str] = None
    tls_cipher: Optional[str] = None
    load_id: Optional[str] = None
    load_timestamp: datetime = Field(default_factory=datetime.utcnow)

    def generate_load_id(self):
        # Create a dictionary of all fields except load_id and load_timestamp
        fields_dict = {
            k: v
            for k, v in self.dict().items()
            if k not in ["load_id", "load_timestamp"]
        }
        # Convert to a JSON string for consistent ordering
        fields_json = json.dumps(fields_dict, sort_keys=True, default=str)
        # Generate SHA256 hash
        return hashlib.sha256(fields_json.encode()).hexdigest()

    def __init__(self, **data):
        super().__init__(**data)
        self.load_id = self.generate_load_id()

    @classmethod
    def duckdb_schema(cls, table_name="pypi_file_downloads"):
        return f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            timestamp TIMESTAMP WITH TIME ZONE,
            country_code VARCHAR,
            url VARCHAR,
            project VARCHAR,
            file STRUCT("filename" VARCHAR, "project" VARCHAR, "version" VARCHAR, "type" VARCHAR),
            details STRUCT("installer" STRUCT("name" VARCHAR, "version" VARCHAR), "python" VARCHAR, "implementation" STRUCT("name" VARCHAR, "version" VARCHAR), "distro" STRUCT("name" VARCHAR, "version" VARCHAR, "id" VARCHAR, "libc" STRUCT("lib" VARCHAR, "version" VARCHAR)), "system" STRUCT("name" VARCHAR, "release" VARCHAR), "cpu" VARCHAR, "openssl_version" VARCHAR, "setuptools_version" VARCHAR, "rustc_version" VARCHAR, "ci" BOOLEAN),
            tls_protocol VARCHAR,
            tls_cipher VARCHAR,
            load_id VARCHAR,
            load_timestamp TIMESTAMP WITH TIME ZONE
        )
        """

    @classmethod
    def pyarrow_schema(cls):
        return pa.schema(
            [
                pa.field("timestamp", pa.timestamp("us", tz="UTC")),
                pa.field("country_code", pa.string()),
                pa.field("url", pa.string()),
                pa.field("project", pa.string()),
                pa.field(
                    "file",
                    pa.struct(
                        [
                            pa.field("filename", pa.string()),
                            pa.field("project", pa.string()),
                            pa.field("version", pa.string()),
                            pa.field("type", pa.string()),
                        ]
                    ),
                ),
                pa.field(
                    "details",
                    pa.struct(
                        [
                            pa.field(
                                "installer",
                                pa.struct(
                                    [
                                        pa.field("name", pa.string()),
                                        pa.field("version", pa.string()),
                                    ]
                                ),
                            ),
                            pa.field("python", pa.string()),
                            pa.field(
                                "implementation",
                                pa.struct(
                                    [
                                        pa.field("name", pa.string()),
                                        pa.field("version", pa.string()),
                                    ]
                                ),
                            ),
                            pa.field(
                                "distro",
                                pa.struct(
                                    [
                                        pa.field("name", pa.string()),
                                        pa.field("version", pa.string()),
                                        pa.field("id", pa.string()),
                                        pa.field(
                                            "libc",
                                            pa.struct(
                                                [
                                                    pa.field("lib", pa.string()),
                                                    pa.field("version", pa.string()),
                                                ]
                                            ),
                                        ),
                                    ]
                                ),
                            ),
                            pa.field(
                                "system",
                                pa.struct(
                                    [
                                        pa.field("name", pa.string()),
                                        pa.field("release", pa.string()),
                                    ]
                                ),
                            ),
                            pa.field("cpu", pa.string()),
                            pa.field("openssl_version", pa.string()),
                            pa.field("setuptools_version", pa.string()),
                            pa.field("rustc_version", pa.string()),
                            pa.field("ci", pa.bool_()),
                        ]
                    ),
                ),
                pa.field("tls_protocol", pa.string()),
                pa.field("tls_cipher", pa.string()),
                pa.field("load_id", pa.string()),
                pa.field("load_timestamp", pa.timestamp("us", tz="UTC")),
            ]
        )


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


class TableValidationError(Exception):
    """Custom exception for Table validation errors."""

    pass


def validate_table(table: pa.Table, model: Type[BaseModel]):
    """
    Validates each row of a PyArrow Table against a Pydantic model.
    Raises TableValidationError if any row fails validation.

    :param table: PyArrow Table to validate.
    :param model: Pydantic model to validate against.
    :raises: TableValidationError
    """
    errors = []

    for i in range(table.num_rows):
        row = {column: table[column][i].as_py() for column in table.column_names}
        try:
            model(**row)
        except ValidationError as e:
            errors.append(f"Row {i} failed validation: {e}")

    if errors:
        error_message = "\n".join(errors)
        raise TableValidationError(
            f"Table validation failed with the following errors:\n{error_message}"
        )
