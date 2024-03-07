import asyncio

from temporalio.client import Client
from temporalio.worker import Worker

from activities import parse_input, download_activity, get_transcript_activity, pre_process_activity, read_doc_activity, \
    indexing_activity

from workflows import PPTWorkflow, DocsWorkflow, MediaWorkflow, MainWorkflow


async def main() -> None:
    client: Client = await Client.connect("localhost:7233", namespace="default")
    # Run the worker
    worker: Worker = Worker(
        client,
        task_queue="Some_QUEUE",
        workflows=[PPTWorkflow, DocsWorkflow, MediaWorkflow, MainWorkflow],
        activities=[parse_input, download_activity, get_transcript_activity, pre_process_activity, read_doc_activity, indexing_activity],
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
