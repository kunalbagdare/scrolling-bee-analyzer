import sys
import requests
from requests.adapters import HTTPAdapter
from scrapy import Selector
import pandas as pd
from contextlib import contextmanager
import openai
from loguru import logger



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

    logger.info("Requesting on url : "+url)
    session = requests.Session()
    session.mount(url, HTTPAdapter(max_retries=5))
    selector = Selector(text=session.get(url).text)
    return selector


def create_csv(product_list):
    """
    This function takes a list of products and creates a CSV file named "scrolling_bee.csv" with the
    product information.
    
    :param product_list: The parameter `product_list` is a list of dictionaries where each dictionary
    represents a product and contains information about the product such as its name, price,
    description, etc. The function `create_csv` takes this list and converts it into a CSV file named
    "scrolling_bee.csv" with each
    """

    df = pd.DataFrame(product_list)
    df.to_csv('scrolling_bee.csv',index=False)

@contextmanager
def openai_request(messages,temperature=0.2,max_tokens=500):
    """
    This function sends a request to OpenAI's GPT-3.5-Turbo model to generate a response based on given
    messages, temperature, and maximum tokens.
    
    :param messages: This parameter takes a list of messages exchanged between the user and the AI
    model. These messages are used as context for generating the next response
    :param temperature: The temperature parameter controls the creativity of the generated text. A
    higher temperature value will result in more diverse and unexpected responses, while a lower
    temperature value will result in more predictable and conservative responses. The default value of
    0.2 is relatively low, which means that the generated text will be more predictable
    :param max_tokens: The maximum number of tokens (words or symbols) that the GPT-3 model will
    generate in response to the input messages. This parameter helps to control the length of the
    response, defaults to 500 (optional)
    """
    
    completion = openai.ChatCompletion.create(
    model="gpt-3.5-turbo",
    messages = messages,
    temperature=temperature,
    max_tokens=max_tokens)
    output = completion.choices[0].message['content']

    yield output





def custom_logger():
    """
    This function sets up a custom logger with specific formatting and log levels.
    :return: The function `custom_logger()` is returning the `logger` object after modifying its
    configuration.
    """

    logger.remove(0)
    logger.add(sys.stdout, format="<bold><green>[{time:HH:mm:ss}]</green></bold> [<bold><level>{level}</level></bold>] {message}")
    logger.level("INFO", color="<blue>")
    logger.level("WARNING", color="<yellow>")
    logger.level("ERROR", color="<red>")

    return logger
