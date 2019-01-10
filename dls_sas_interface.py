"""Helper Module to Access SAS APIs for send heartbeat Messages.
Example Usage:

import dls_sas_interface

req_interface = dls_sas_interface.DlsSasInterface(sasm_url) #initiate the object
(status,response) = req_interface.sendHeartbeatRequest(reqJson) #Call the send function with HB request body JSON 
if status == 200: #success
    process response
else: #error occured even after retrying
    hanldle error
"""

import requests
import json
import decimal


# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


class DlsSasInterface(object):
    def __init__(self, sasm_url, sasm_port=443, sasm_api_version='v1.0'):
        """

        :rtype: object
        """
        self._sasm_url = sasm_url
        self._sasm_port = sasm_port
        self._sasm_api_version = sasm_api_version
        self._baseurl = 'https://' + self._sasm_url + ':' + str(
            self._sasm_port) + '/esc/' + self._sasm_api_version + '/'
        self._num_retry = 3

    def sendHeartbeatRequest(self, reqJson):
        for i in range(self._num_retry):
            (status, response) = (None, None)
            try:
                status, response = self.postHeartbeatRequest(reqJson)
                if status == 200:
                    return status, response
            except Exception as e:
                print(e)
                if status is None:
                    status = 400
                    response = str(e)
        # error after retries
        return (status, response)

    def postHeartbeatRequest(self, reqJson):
        print(reqJson)
        requrl = self._baseurl + 'heartbeat'
        print(requrl)
        headers = {'Content-type': 'application/json'}
        response = requests.post(requrl, json=reqJson, headers=headers)
        print(response)
        print(response.status_code)
        responseMsg = None
        if response.text:
            responseMsg = response.text
        if response.status_code == 200:
            print(response.json())
        else:
            try:
                responseMsg = response.json()
                print(responseMsg)
            except Exception as e:
                print(e)
                print('Response Message is Not JSON')
                responseMsg = 'Error in SAS API - ' + str(e)
        if response.status_code == 200:
            return (response.status_code, response.json())
        else:
            return (response.status_code, responseMsg)
