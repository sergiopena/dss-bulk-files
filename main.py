from sws_api_client import SwsApiClient, TaskManager

from common import process

if __name__ == '__main__':
    sws_client = SwsApiClient.auto()
    tasks = TaskManager(sws_client)
    current_task = tasks.get_task(sws_client.current_task_id)
    if current_task is None:
        raise ValueError("Current task not found. Please run this script within a valid task context.")

    if current_task.info.input.get('payload') is None:
        raise ValueError("Payload is missing in the current task input.")

    if current_task.info.input.get('payload').get('parameters') is None:
        raise ValueError("Parameters are missing in the current task input payload.")

    parameters = current_task.info.input['payload']['parameters']

    if parameters.get('DATAFLOW_URL') is None or parameters.get('S3_BUCKET') is None or parameters.get(
            'FILENAME') is None:
        raise ValueError("Missing required parameters: DATAFLOW_URL, S3_BUCKET, FILENAME")

    process(dataflow_url=parameters['DATAFLOW_URL'],
            s3_bucket=parameters['S3_BUCKET'],
            filename=parameters['FILENAME'])
