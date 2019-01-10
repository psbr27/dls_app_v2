import boto3
import json


class sqsBoto(object):
    def __init__(self, region, queue):
        self.sqs_obj = boto3.client('sqs', region_name=region)
        self.queue = queue

    def poll_queue_for_messages(self):
        """Function polls SQS queue for sensor health messages
        :param py_sqs: pyboto3.sqs
        :param queue:  queue url
        :return: sensor health message from queue
        """
        return self.sqs_obj.receive_message(
            QueueUrl=self.queue,
            MaxNumberOfMessages=1
        )

    def delete_message_from_queue(self, receipt_handle):
        """Function delete the message from queue after processing
        :param receipt_handle:
        :param py_sqs: pyboto3.sqs
        :param queue: queue url
        :return: None
        """
        self.sqs_obj.delete_message(
            QueueUrl=self.queue,
            ReceiptHandle=receipt_handle
        )

    def process_message_from_queue(self):
        """Function process the sensor health messages from SQS
        :param client: pyboto3.sqs
        :param queue: sqs url
        :return: success -> sensor id and timestamp; failure -> None
        """
        queued_messages = self.poll_queue_for_messages()
        if 'Messages' in queued_messages and len(queued_messages['Messages']) >= 1:
            for message in queued_messages['Messages']:
                temp = message['Body']
                val = json.loads(temp)
                if 'error' not in val:  # handle health not rx error
                    try:
                        self.delete_message_from_queue(message['ReceiptHandle'])
                        return val['sensorId'], val['timestamp']
                    except KeyError:
                        return None, None
                else:
                    self.delete_message_from_queue(message['ReceiptHandle'])
                    return None, None
        else:
            return None, None
