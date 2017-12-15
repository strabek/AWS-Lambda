from __future__ import print_function

import boto3
import json
import requests
import logging
import os
import datetime

listUrl = os.environ['listUrl']
storyUrl = 'https://hacker-news.firebaseio.com/v0/item/{}.json?print=pretty'

# DynamoDB settings
dynamoDB_table = os.environ['DynamoDB_table']
dynamoDBClient = boto3.client('dynamodb')

logger = logging.getLogger()
logger.setLevel(logging.INFO) 

def lambda_handler(event, context):
    
    storyIds = makeRequest(listUrl)

    for storyId in storyIds:
        story = makeRequest(storyUrl.format(storyId))
        logger.info(story)
        
        if  'id' in story:
            id = str(story['id'])
        else:
            id = ' '
        if 'title' in story:
            title = str(story['title'])
        else:
            title = ' '
        if 'url' in story:
            url = str(story['url'])
        else:
            url = ' '
        if 'time' in story:
            publishedAt = datetime.datetime.fromtimestamp(story['time']).strftime('%Y-%m-%d %H:%M:%S')
        else:
            publishedAt = ' '

        response = dynamoDBClient.put_item(
            TableName=dynamoDB_table,
            Item={
                "id": { "S": str(id) },
                "title": { "S": str(title) },
                "url": { "S": str(url) },
                "publishedAt": { "S": str(publishedAt) }
            }
        )
            

    return

def makeRequest(url):
    try:
        response = requests.get(url)
    except requests.exceptions.Timeout as e:
        # The request timed out.
        # Catching this error will catch both
        # :exc:`~requests.exceptions.ConnectTimeout` and
        # :exc:`~requests.exceptions.ReadTimeout` errors.

        logger.error("Error: {}".format(e))
        return
    except requests.exceptions.HTTPError as e:
        logger.error("Error: {}".format(e))
        return
    except requests.exceptions.RequestException as e:
        logger.error("Error: {}".format(e))
        return

    return response.json()