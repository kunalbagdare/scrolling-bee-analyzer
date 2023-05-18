import os
import sys
import pymongo
import requests
from requests.adapters import HTTPAdapter
from scrapy import Selector
import pandas as pd
from contextlib import contextmanager
import openai
from loguru import logger
import time
from dotenv import load_dotenv

load_dotenv()


def custom_logger():
    """
    This function sets up a custom logger with specific formatting and log levels.
    :return: The function `custom_logger()` is returning the `logger` object after modifying its
    configuration.
    """
    try:
        logger.remove(0)
        logger.add(sys.stdout, format="<bold><green>[{time:HH:mm:ss}]</green></bold> [<bold><level>{level}</level></bold>] {message}")
        logger.level("INFO", color="<blue>")
        logger.level("WARNING", color="<yellow>")
        logger.level("ERROR", color="<red>")
        logger.level("DEBUG", color="<green>")
    except ValueError:
        pass
    
    return logger

logger = custom_logger()

def generate_selector(url):
    """
    This function creates a session, mounts the URL with a maximum of 5 retries, and returns a selector
    object from the HTML content of the URL.
    
    :param url: The URL of the webpage that we want to make a request to and extract data from
    :return: a Selector object, which is created by parsing the HTML content of the response obtained by
    making a GET request to the input URL using the requests library. The Selector object allows for
    easy selection and extraction of specific elements from the HTML content using XPath or CSS
    selectors.
    """

    logger.debug("Requesting on url : "+url)
    session = requests.Session()
    session.mount(url, HTTPAdapter(max_retries=5))
    selector = Selector(text=session.get(url).text)
    return selector


def create_csv(product_list,filename):
    """
    This function creates a CSV file from a given list of products and saves it in a specified
    directory.
    
    :param product_list: It is a list of dictionaries or a list of lists containing the data that needs
    to be written to a CSV file
    :param filename: The name of the CSV file that will be created. It should be a string without the
    ".csv" extension
    """
    
    df = pd.DataFrame(product_list)

    if not os.path.exists('data'):
        os.mkdir('data')

    df.to_csv(f'data/{filename}.csv',index=False)

@contextmanager
def openai_request(messages,temperature=0.2,max_tokens=500):
    
    openai.api_key = os.getenv("OPENAI_API_KEY")

    while True:
        try:
            completion = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages = messages,
            temperature=temperature,
            max_tokens=max_tokens)
            output = completion.choices[0].message['content']

            yield output
            break
        except openai.error.RateLimitError:
            logger.info('Sleeping for 20 seconds')
            time.sleep(20)


@contextmanager
def mongo_connection(database_name,collection_name):
    """
    This function establishes a connection to a MongoDB database and returns a generator object for a
    specified collection within that database.
    
    :param database_name: The name of the MongoDB database you want to connect to
    :param collection_name: The name of the collection in the MongoDB database that you want to connect
    to
    """
    connection_string = os.getenv('MONGODB_CONNECTION','')
    client = pymongo.MongoClient(connection_string,ssl=False)
    db = client[database_name]
    collection = db[collection_name]
    yield collection
    client.close()


def time_calculator(function):
    """
    This is a Python decorator function that calculates the total time taken by a given function to
    execute.
    
    :param function: The function parameter is a function that will be passed as an argument to the
    time_calculator decorator. The decorator will then wrap this function with additional functionality
    to calculate the time it takes to execute the function
    :return: The `time_calculator` function returns a new function called `wrapper`, which wraps around
    the original function passed as an argument. The `wrapper` function calculates the time taken to
    execute the original function and prints it.
    """
    def wrapper(*args, **kwargs):
        start_time = time.time()
        function(*args, **kwargs)
        end_time = time.time()
        logger.info(f"Total time taken: {str(round(end_time - start_time,2))} seconds") 
    return wrapper





