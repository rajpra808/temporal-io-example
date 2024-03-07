import asyncio
import traceback

from temporalio.client import Client, WorkflowFailureError
from workflows import MainWorkflow


async def main() -> None:
    # Create client connected to server at the given address
    client: Client = await Client.connect("localhost:7233")

    try:
        result = await client.execute_workflow(
            MainWorkflow.run,
            "inputs",
            id="main-req-105",
            task_queue="Some_QUEUE"
        )

        print(f"Result: {result}")

    except WorkflowFailureError:
        print("Got expected exception: ", traceback.format_exc())


if __name__ == "__main__":
    asyncio.run(main())
