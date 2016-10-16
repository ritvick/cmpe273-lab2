from dateutil import parser
import operator
import re
import requests
import logging
logging.basicConfig(level=logging.DEBUG)

from spyne import Application, rpc, ServiceBase, \
    Integer, Unicode

from spyne import Iterable

from spyne.protocol.http import HttpRpc
from spyne.protocol.json import JsonDocument

from spyne.server.wsgi import WsgiApplication

from streetaddress import StreetAddressFormatter, StreetAddressParser




class CheckCrimeClass(ServiceBase):
    @rpc(Unicode, Unicode,Unicode, _returns=Iterable(Unicode))
    def checkcrime(ctx, lat,lon, radius):
        timeTwelveOneAM = parser.parse("00:01 AM")
        timeThreeOneAM = parser.parse("03:01 AM")
        timeSixOneAM = parser.parse("06:01 AM")
        timeNineOneAM = parser.parse("09:01 AM")
        timeTwelveOnePM = parser.parse("12:01 PM")
        timeThreeOnePM = parser.parse("03:01 PM")
        timeSixOnePM = parser.parse("06:01 PM")
        timeNineOnePM = parser.parse("09:01 PM")
        timeElevenFiftyNine = parser.parse("11:59 PM")
        timeTwelveMidnight = parser.parse("00:00 AM")

        addr_parser = StreetAddressParser()

        parameters = {"lat": lat, "lon": lon, "radius": radius, "key": "."}
        response = requests.get("https://api.spotcrime.com/crimes.json?", params = parameters)
        data = response.json()
        crimes = data["crimes"]
        crimeType = {}
        event_time_count = {
            '12:01am-3am' : 0,
            "3:01am-6am" : 0,
            "6:01am-9am" : 0,
            "9:01am-12noon" : 0,
            "12:01pm-3pm" : 0,
            "3:01pm-6pm" : 0,
            "6:01pm-9pm" : 0,
            "9:01pm-12midnight" : 0
        }
        streetCount = {}
        totalCrime = 0
        for crime in crimes:
            dateTime = parser.parse(crime["date"])
            totalCrime = totalCrime + 1
            typeOfCrime = crime["type"]
            if typeOfCrime in crimeType:
                crimeType[typeOfCrime] = crimeType[typeOfCrime] + 1
            else:
                crimeType[typeOfCrime] = 1 

            if dateTime.time() >= timeTwelveOneAM.time() and dateTime.time() < timeThreeOneAM.time():
                event_time_count['12:01am-3am'] = event_time_count['12:01am-3am'] + 1
            elif dateTime.time() >= timeThreeOneAM.time() and dateTime.time() < timeSixOneAM.time():
                event_time_count['3:01am-6am'] =  event_time_count['3:01am-6am'] + 1
            elif dateTime.time() >= timeSixOneAM.time() and dateTime.time() < timeNineOneAM.time():
                event_time_count['6:01am-9am'] =  event_time_count['6:01am-9am'] + 1
            elif dateTime.time() >= timeNineOneAM.time() and dateTime.time() < timeTwelveOnePM.time():
                event_time_count['9:01am-12noon'] =  event_time_count['9:01am-12noon'] + 1
            elif dateTime.time() >= timeTwelveOnePM.time() and dateTime.time() < timeThreeOnePM.time():
                event_time_count['12:01pm-3pm'] =  event_time_count['12:01pm-3pm'] + 1
            elif dateTime.time() >= timeThreeOnePM.time() and dateTime.time() < timeSixOnePM.time():
                event_time_count['3:01pm-6pm'] =  event_time_count['3:01pm-6pm'] + 1     
            elif  dateTime.time() >= timeSixOnePM.time() and dateTime.time() < timeNineOnePM.time():
                event_time_count['6:01pm-9pm'] = event_time_count['6:01pm-9pm'] + 1
            elif  dateTime.time() >= timeNineOnePM.time() and dateTime.time() <= timeElevenFiftyNine.time():
                event_time_count['9:01pm-12midnight'] = event_time_count['9:01pm-12midnight'] + 1
            elif  dateTime.time() == timeTwelveMidnight.time():
                event_time_count['9:01pm-12midnight'] = event_time_count['9:01pm-12midnight'] + 1    

            addr = addr_parser.parse(crime["address"])

            street = addr["street_full"]
            street = re.sub("BLOCK OF ","",street,count=2)
            street = re.sub("BLOCK ","",street,count=2)

            if street in streetCount:
                streetCount[street] = streetCount[street] + 1
            else:
                streetCount[street] = 1

        sorted_x = sorted(streetCount.items(), key=operator.itemgetter(1))
        
        sorted_x.reverse()

        output = {
        "crime_type_count" : crimeType,
        "total_crime" : totalCrime,
        "event_time_count": event_time_count,
        "the_most_dangerous_streets" : [sorted_x[0][0], sorted_x[1][0], sorted_x[2][0]]
        
        }
        
        yield output    

application = Application([CheckCrimeClass],
    tns='spyne.examples.hello',
    in_protocol=HttpRpc(validator='soft'),
    out_protocol=JsonDocument()
)

if __name__ == '__main__':
    # You can use any Wsgi server. Here, we chose
    # Python's built-in wsgi server but you're not
    # supposed to use it in production.
    from wsgiref.simple_server import make_server

    wsgi_app = WsgiApplication(application)
    server = make_server('0.0.0.0', 8000, wsgi_app)
    server.serve_forever()