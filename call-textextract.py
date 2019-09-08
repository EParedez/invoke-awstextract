import boto3
import time
import logging
from datetime import datetime

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

# simple text detection pdf

time_start = datetime.now()

def start_job(s3BucketName, objectName):
    response = None
    client = boto3.client('textract')
    response = client.start_document_text_detection(
        DocumentLocation={
            'S3Object': {
                'Bucket': s3BucketName,
                'Name': objectName
            }
        })
    logging.info('start job')

    return response["JobId"]


def is_job_complete(jobId):
    time.sleep(5)
    client = boto3.client('textract')
    response = client.get_document_text_detection(JobId=jobId)
    status = response["JobStatus"]
    logging.info("Job status: {}".format(status))

    while status == "IN_PROGRESS":
        time.sleep(5)
        response = client.get_document_text_detection(JobId=jobId)
        status = response["JobStatus"]
        logging.info("Job status: {}".format(status))

    if status == 'SUCCEEDED':
        time_end = datetime.now()
        time_result = time_end - time_start
        logging.info('Time in seconds: ' + str(time_result.seconds) )
        logging.info('Job Finish')


    return status


def get_job_results(jobId):
    pages = []

    time.sleep(5)

    client = boto3.client('textract')
    response = client.get_document_text_detection(JobId=jobId)

    pages.append(response)
    logging.info("Resultset page recieved: {}".format(len(pages)))
    nextToken = None
    if 'NextToken' in response:
        nextToken = response['NextToken']

    while (nextToken):
        time.sleep(5)

        response = client.get_document_text_detection(JobId=jobId, NextToken=nextToken)

        pages.append(response)
        logging.info("Resultset page recieved: {}".format(len(pages)))
        nextToken = None
        if ('NextToken' in response):
            nextToken = response['NextToken']

    return pages


def run_read_text(bucketName, file):
    # Document
    s3BucketName = bucketName
    documentName = file

    jobId = start_job(s3BucketName, documentName)
    logging.info("Started job with id: {}".format(jobId))
    if is_job_complete(jobId):
        response = get_job_results(jobId)

    # print(response)
    result = ""

    # Print detected text
    for resultPage in response:
        for item in resultPage["Blocks"]:
            if item["BlockType"] == "LINE":
                #print ('\033[94m' + item["Text"] + '\033[0m')
                result += item["Text"] + "\n"

    return result


# add bucketname and file
text = run_read_text("", "")
logging.info(text)