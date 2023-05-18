import json
import os
import click
import pandas as pd
from pipelines.utils import custom_logger, openai_request, time_calculator, mongo_connection

logger = custom_logger()


def request_prompt(comment_list):
    """
    The function takes a list of comments as input and uses OpenAI to generate a summarized output in
    JSON format with positive, negative, suggestion, and score sections.
    
    :param comment_list: The list of reviews/comments for a particular honey selling company's product
    :return: The function `request_prompt` returns the result of the OpenAI API request, which is a JSON
    object containing the summarized output of the given reviews in the specified format.
    """
    
    system = '''
    Act as a smart business analyst and identify key insights and features from given reviews of a particular honey of honey selling company. Provide the summarized output in 3 sections - Positive, Negative and Recommendation/Suggestion.
    All the sections should have a short paragraph.
    Provide overall score to the product based on reviews on a scale of 1 to 10 with being worst as 1 and being best at 10.
    Note - Do not copy paste reviews as it is in output. Do not mention anything other then fields specified. if any of the section has no output just say NA.
    Output format-
    {
    "Positive":<output>,
    "Negative":<output>,
    "Suggestion":<output>,
    "Score":<score>
    }
    '''
    assistant_1 = 'Any other parameters ?'
    user_1 = 'Yes. Some of the reviews wont make any sense or have meaning and maybe just have the product name as it is so ignore them and also provide output such that it can be used for displaying insights to dashboard. Do not mention anyting other then fields.'
    assistant_2 = 'Okay I will not provide any other information then the json fields and strictly follow the json format. Provide the review list.'

    comment_bullet_list = ['• '+x for x in comment_list]
    comment_bullets = ''.join(comment_bullet_list)

    

    messages=[
        {"role": "system", "content": system},
        {"role": "assistant", "content": assistant_1},
        {"role": "user", "content":user_1 },
        {"role": "assistant", "content": assistant_2},
        {"role": "user", "content":comment_bullets }
            ]
    
    with openai_request(messages=messages,temperature=0.7,max_tokens=1000) as output:
        result = output
    return result
    
    
def output_cleaner(output,product_name,rating,no_of_reviews):
    """
    The function takes in an output, product name, rating, and number of reviews, cleans the output,
    adds the product name, rating, and number of reviews to the output, and returns the result.
    
    :param output: The output string that needs to be cleaned and converted to a JSON object
    :param product_name: The name of the product being reviewed
    :param rating: The rating of a product, typically on a scale of 1-5, that indicates the overall
    satisfaction level of customers who have purchased and used the product
    :param no_of_reviews: The number of reviews for a product
    :return: a dictionary with the cleaned output, along with additional information such as the product
    name, rating, and number of reviews.
    """

    try:
        if 'Note:' in output:
            text = output.split('Note:')[0]
        elif 'Explanation:' in output:
            text = output.split('Explanation:')[0]
        else:
            text = output
        result = json.loads(text)
        result['product_name'] = product_name
        result['rating'] = rating
        result['no_of_reviews'] = no_of_reviews

    except json.decoder.JSONDecodeError:
        print(output)
        raise
    return result
    
    
@click.command()
@click.option("--database_name", help="Name of the database")
@click.option("--collection_name", help="Name of the collection")
@time_calculator
def main(database_name,collection_name):
    """
    This function reads data from a CSV file, processes it, prompts for user input, cleans the output,
    and inserts the results into a MongoDB collection.
    
    :param database_name: The name of the MongoDB database where the data will be stored
    :param collection_name: The name of the collection in the MongoDB database where the scraped data
    will be stored
    """

    csv_location = 'scrapper/data/'
    csv_file = [file for file in os.listdir(csv_location) if file.split('.')[-1]=='csv'][0]

    result_database = []
    df = pd.read_csv(csv_location+csv_file)
    df = df.reset_index()

    names = df['NAME'].to_list()
    ratings = df['RATING'].to_list()
    no_of_reviews = df['NO_OF_REVIEWS'].to_list()

    all_reviews = df['REVIEWS'].to_list()
    reviews = [eval(x) for x in all_reviews]

    for i,review_list in enumerate(reviews):
        logger.info(f'========================= Batch {str(i+1)} =======================================')
        output = request_prompt(review_list)
        result = output_cleaner(output,names[i],ratings[i],no_of_reviews[i])
        result_database.append(result)

    with mongo_connection(database_name,collection_name) as collection:
        collection.insert_many(result_database)
    

if __name__ == "__main__":
    main()
