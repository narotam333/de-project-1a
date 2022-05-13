import sys
import time

# Importing library to get device info from user agent
from user_agents import parse

# Importing library to get geo-location from Client IP
import geocoder

import boto3
from botocore.exceptions import ClientError
import logging

def generate_csv(bucketName, logFileName):
    src_file = logFileName

    timestr = time.strftime("%Y%m%d-%H%M%S")
    outFileName = 'weblog_'+timestr+'.csv'
    f = open(outFileName,'w')
 
    s3_client = boto3.client('s3')
    log_obj = s3_client.get_object(Bucket = bucketName, Key = logFileName)
 
    logging.info('Starting reading log file for conversion to csv along with IP and user_agent conversion...')
    
    for line in log_obj["Body"].iter_lines():
        logEntry = line.decode("utf-8").split()
        clientIP = logEntry[0]
        user = logEntry[2]
        tm = logEntry[3]
        userAgent = line.decode("utf-8").split('\"')[5]
    
        response = geocoder.ip(clientIP)
        location = response.country

        user_agent = parse(userAgent)
        device = user_agent.device.family
    
        businessDate = time.strftime("%Y-%m-%d")
        f.write('%s,%s,%s,%s,%s\n' % (location, user, tm, device, businessDate))
        f.flush()

    logging.info('Conversion completed...')
    return outFileName


if __name__ == '__main__':
    bucketName = sys.argv[1]
    logFileName = sys.argv[2]
    generate_csv(bucketName, logFileName)
