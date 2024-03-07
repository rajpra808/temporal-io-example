import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError

with workflow.unsafe.imports_passed_through():
    from activities import parse_input, download_activity, get_transcript_activity, pre_process_activity,read_doc_activity,indexing_activity
    from model import IndividualInput, PreProcessInput, IndexingInput, IndexingOutput, Input


@workflow.defn
class MediaWorkflow:
    @workflow.run
    async def run(self, input: IndividualInput):
        print("PPTWorkflow: ", input.__dict__)

        retry_policy = RetryPolicy(
            maximum_attempts=3,
            maximum_interval=timedelta(seconds=2),
            non_retryable_error_types=["CustomRPError"],
        )

        try:
            # Download code
            download_output: PreProcessInput = await workflow.execute_activity(
                download_activity,
                input,
                start_to_close_timeout=timedelta(seconds=15),
                retry_policy=retry_policy,
            )

            # Preprocess
            pre_process_out: IndexingInput = await workflow.execute_activity(
                get_transcript_activity,
                download_output,
                start_to_close_timeout=timedelta(seconds=15),
                retry_policy=retry_policy,
            )

            indexing_out: IndexingOutput = await workflow.execute_activity(
                indexing_activity,
                pre_process_out,
                start_to_close_timeout=timedelta(seconds=15),
                retry_policy=retry_policy,
            )
            return indexing_out
        except ActivityError as err:
            workflow.logger.exception("Something went wrong", err)


@workflow.defn
class PPTWorkflow:
    @workflow.run
    async def run(self, req: IndividualInput):
        print("PPTWorkflow: ", req.__dict__)

        retry_policy = RetryPolicy(
            maximum_attempts=3,
            maximum_interval=timedelta(seconds=2),
            non_retryable_error_types=["CustomRPError"],
        )

        try:
            # Download code
            download_output: PreProcessInput = await workflow.execute_activity(
                download_activity,
                req,
                start_to_close_timeout=timedelta(seconds=15),
                retry_policy=retry_policy,
            )

            # Preprocess
            pre_process_out: IndexingInput = await workflow.execute_activity(
                pre_process_activity,
                download_output,
                start_to_close_timeout=timedelta(seconds=15),
                retry_policy=retry_policy,
            )

            indexing_out: IndexingOutput = await workflow.execute_activity(
                indexing_activity,
                pre_process_out,
                start_to_close_timeout=timedelta(seconds=15),
                retry_policy=retry_policy,
            )
            return indexing_out
        except ActivityError as err:
            workflow.logger.exception("Something went wrong", err)


@workflow.defn
class DocsWorkflow:
    @workflow.run
    async def run(self, req: IndividualInput):
        print("DocsWorkflow: ", req.__dict__)

        retry_policy = RetryPolicy(
            maximum_attempts=3,
            maximum_interval=timedelta(seconds=2),
            non_retryable_error_types=["CustomRPError"],
        )

        try:
            # Download code
            download_output: PreProcessInput = await workflow.execute_activity(
                download_activity,
                req,
                start_to_close_timeout=timedelta(seconds=15),
                retry_policy=retry_policy,
            )

            # Preprocess
            pre_process_out: IndexingInput = await workflow.execute_activity(
                read_doc_activity,
                download_output,
                start_to_close_timeout=timedelta(seconds=15),
                retry_policy=retry_policy,
            )

            indexing_out: IndexingOutput = await workflow.execute_activity(
                indexing_activity,
                pre_process_out,
                start_to_close_timeout=timedelta(seconds=15),
                retry_policy=retry_policy,
            )
            return indexing_out
        except ActivityError as err:
            workflow.logger.exception("Something went wrong", err)


@workflow.defn
class MainWorkflow:
    @workflow.run
    async def run(self, inputs: str):
        print("Hello World")

        retry_policy = RetryPolicy(
            maximum_attempts=3,
            maximum_interval=timedelta(seconds=2),
            non_retryable_error_types=["CustomRPError"],
        )

        try:
            # Download code
            input2 = await workflow.execute_activity(
                parse_input,
                inputs,
                start_to_close_timeout=timedelta(seconds=15),
                retry_policy=retry_policy,
            )
            # print(input2.__dict__)
            data_media: IndividualInput = IndividualInput(
                request_type="media",
                file_url="url",
                document_id="123"
            )

            data_ppt: IndividualInput = IndividualInput(
                request_type="ppt",
                file_url="url",
                document_id="123"
            )

            data_docs: IndividualInput = IndividualInput(
                request_type="docs",
                file_url="url",
                document_id="123"
            )

            promises = []
            for fl in range(0, 2, 1):
                promises.append(workflow.execute_child_workflow(MediaWorkflow.run, data_media, id=f"hello_media{fl}"))

            for fl in range(0, 2, 1):
                promises.append(workflow.execute_child_workflow(PPTWorkflow.run, data_ppt, id=f"hello_ppt{fl}"))

            for fl in range(0, 2, 1):
                promises.append(workflow.execute_child_workflow(DocsWorkflow.run, data_docs, id=f"hello_docs{fl}"))

            results = await asyncio.gather(*promises)
            print(results)
            return input2
        except Exception as e:
            workflow.logger.exception("Something went wrong", e)
