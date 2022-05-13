#!/usr/bin/env python3

# #import argparse
from base64 import decode
import sys
import time

# Importing library to get device info from user agent
from user_agents import parse

# Importing library to get geo-location from Client IP
import geocoder
import logging

def generate_csv(logFileName, outFileName):
    src_file = logFileName

    in_f = open(src_file,'r')
    f = open(outFileName,'w')
    logging.info('Starting reading log file for conversion to csv along with IP and user_agent conversion...')

    for line in in_f:
        print(line)
        logEntry = line.split()
        print(logEntry)
        clientIP = logEntry[0]
        print(clientIP)
        user = logEntry[2]
        tm = logEntry[3]
        userAgent = line.split('\"')[5]
    
        response = geocoder.ip(clientIP)
        #response = DbIpCity.get(clientIP, api_key='free')
        location = response.country

        user_agent = parse(userAgent)
        device = user_agent.device.family
    
        businessDate = time.strftime("%Y-%m-%d")
        f.write('%s,%s,%s,%s,%s\n' % (location, user, tm, device, businessDate))
        f.flush()

    logging.info('Conversion completed...')
    return outFileName


if __name__ == '__main__':
    logFileName = sys.argv[1]
    outFileName = sys.argv[2]
    generate_csv(logFileName, outFileName)
