from dataclasses import dataclass
from enum import Enum


class RequestType(Enum):
    PPT_TYPE = "ppt_type"
    MEDIA_TYPE = "media_type"
    DOCS_TYPE = "docs_type"


@dataclass
class IndividualInput:
    document_id: str
    file_url: str
    request_type: str


@dataclass
class PreProcessInput:
    from_download: str
    document_id: str
    file_url: str
    request_type: str


@dataclass
class IndexingInput:
    document_id: str
    from_last_step: str


@dataclass
class IndexingOutput:
    document_id: str
    status: str


@dataclass
class Input:
    task_id: str
    user_id: str
    inputs: [dict]

