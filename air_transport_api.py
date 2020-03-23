import json
import requests


from myDb import myDb

class Air_Transport_Api(myDb):
    def __init__(self):
        # FlightXML API parameters
        self.userName = 'Waave'
        self.apiKey = '71fa3737a4a0d94e3127b568a3bda2d01a8d8821'
        self.base_url = 'http://flightxml.flightaware.com/json/FlightXML2/'

    def get_api_data(self, action, payload=None):
        # make request to API for data
        if payload:
            response = requests.get(
                self.base_url + action, 
                params=payload, 
                auth=(self.userName, self.apiKey))
        else:
            response = requests.get(
                self.base_url + action,
                auth=(self.userName, self.apiKey))

        # return data as JSON
        return response
