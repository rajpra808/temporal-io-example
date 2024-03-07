import time

from temporalio import activity

from model import Input, IndividualInput, PreProcessInput, IndexingInput, IndexingOutput


@activity.defn
async def parse_input(request: str) -> str:
    try:
        print(request)
        activity.logger.info("parse_input: Starting Request", request)
        time.sleep(2)

        activity.logger.info("parse_input: Request Completed")
        return request
    except Exception as e:
        activity.logger.exception("Activity Failed", e)
        raise


@activity.defn
async def download_activity(request: IndividualInput) -> PreProcessInput:
    try:
        print("download_activity: ", request.__dict__)
        activity.logger.info("download_activity: Starting Download", request.__dict__)
        time.sleep(2)
        data: PreProcessInput = PreProcessInput(
            document_id=request.document_id,
            file_url=request.file_url,
            from_download="somewhere-in-ProShort",
            request_type=request.request_type
        )
        activity.logger.info("download_activity: Download Completed")
        return data
    except Exception as e:
        activity.logger.exception("download_activity: Activity Failed", e)
        raise


@activity.defn
async def get_transcript_activity(request: PreProcessInput) -> IndexingInput:
    try:
        print(request.__dict__)
        activity.logger.info("get_transcript_activity: Starting transcript")
        time.sleep(2)
        data: IndexingInput = IndexingInput(
            document_id=request.document_id,
            from_last_step="From get_transcript_activity"
        )
        activity.logger.info("get_transcript_activity: Transcription Completed")
        return data
    except Exception as e:
        activity.logger.exception("get_transcript_activity: Activity Failed", e)
        raise


@activity.defn
async def pre_process_activity(request: PreProcessInput) -> IndexingInput:
    try:
        print("pre_process_activity: Starting Request", request.__dict__)
        activity.logger.info("pre_process_activity: Starting Request", request.__dict__)
        time.sleep(2)
        data: IndexingInput = IndexingInput(
            document_id=request.document_id,
            from_last_step="From pre-processActivity"
        )
        activity.logger.info("pre_process_activity: Request Completed")
        return data
    except Exception as e:
        activity.logger.exception("pre_process_activity: Activity Failed", e)
        raise


@activity.defn
async def read_doc_activity(request: PreProcessInput) -> IndexingInput:
    try:
        activity.logger.info("read_doc_activity: Starting Request")
        time.sleep(2)
        # start child workflows
        data: IndexingInput = IndexingInput(
            document_id=request.document_id,
            from_last_step="From read_doc_activity"
        )
        activity.logger.info("read_doc_activity: Request Completed")
        return data
    except Exception as e:
        activity.logger.exception("read_doc_activity: Activity Failed", e)
        raise


@activity.defn
async def indexing_activity(request: IndexingInput) -> IndexingOutput:
    try:
        print("indexing_activity: Starting Request", request.__dict__)
        activity.logger.info("indexing_activity: Starting Request", request.__dict__)
        time.sleep(2)
        # start child workflows
        data:IndexingOutput = IndexingOutput(
            document_id=request.document_id,
            status="success"
        )
        activity.logger.info("indexing_activity: Request Completed")
        return data
    except Exception as e:
        activity.logger.exception("indexing_activity: Activity Failed", e)
        raise




