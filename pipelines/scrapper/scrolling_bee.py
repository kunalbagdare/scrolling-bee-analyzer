import html
import json
import sys
import time
import traceback
from datetime import datetime

import click
import requests
from bs4 import BeautifulSoup
from scrapy import Selector

sys.path.append('..')
from utils import create_csv, custom_logger, generate_selector,time_calculator

logger = custom_logger()


def calculate_stars(stars):
	"""
	The function calculates the total number of stars based on the input list of stars, taking into
	account full and half stars.
	
	:param stars: The parameter "stars" is expected to be a list of strings, where each string
	represents a star rating. The string can contain either "--on" to indicate a full star or "--half"
	to indicate a half star. The function calculates the total number of stars based on the input list
	and returns
	:return: the total count of stars, including half stars, that are turned on in a list of stars.
	"""

	count_stars = 0
	if len(stars):
		for star in stars:
			if '--on' in star:
					count_stars+=1
			if '--half' in star:
					count_stars+=0.5
	return count_stars





def review_extractor(product_id,last_page):
	"""
	This function extracts reviews for a given product ID from a website and returns them as a list.
	
	:param product_id: The ID of the product for which we want to extract reviews
	:param last_page: The last page of reviews to extract for a given product ID
	:return: a list of reviews for a given product ID and number of pages to scrape.
	"""

	current_page = 1
	all_reviews = []
	review_dates = []

	while current_page <= last_page:

		review_url=f'https://judge.me/reviews/reviews_for_widget?url=blebeehoney.myshopify.com&shop_domain=blebeehoney.myshopify.com&platform=shopify&page={str(current_page)}&per_page=10&product_id={str(product_id)}'
	
		review_response = requests.get(review_url)
		raw_data = json.loads(review_response.text)
		raw_html = html.unescape(raw_data['html'])
		clean_html = str(BeautifulSoup(raw_html, 'html.parser'))
		review_sel = Selector(text=clean_html)

		reviews = review_sel.xpath('//div[@class="jdgm-rev-widg__reviews"]/div').extract()
		
		for review in reviews:
	
			rev = Selector(text=review)
			try:
				title = rev.xpath('//div[@class="jdgm-rev__content"]//b[@class="jdgm-rev__title"]//text()').extract()[0]
			except IndexError as e:
				title = ''

			body = ''.join(rev.xpath('//div[@class="jdgm-rev__content"]//div[@class="jdgm-rev__body"]//text()').extract())

			if title == '':
				all_reviews.append(body)
			else:
				all_reviews.append(title+' | '+body)
			
			try:
				raw_date = rev.xpath('//span[@class="jdgm-rev__timestamp jdgm-spinner"]/@data-content').extract()[0]
				date = datetime.strptime(raw_date, '%Y-%m-%d %H:%M:%S %Z').date().strftime('%Y-%m-%d')
				review_dates.append(date)
			except IndexError as e:
				logger.error('Unable to get date: %s', e)

		current_page += 1

	return all_reviews,review_dates
	
def pv_main(product_url):
	"""
	The function extracts all reviews for a given product URL using web scraping and returns them as a
	list.
	
	:param product_url: The URL of a product page on a website
	:return: The function `pv_main` returns a list of all the reviews for a given product URL.
	"""

	
	pv_sel = generate_selector(product_url)
	last_page_text = pv_sel.xpath('//a[@class="jdgm-paginate__page jdgm-paginate__last-page"]').extract()[0]
	last_page_sel = Selector(text=last_page_text)
	last_page = int(last_page_sel.xpath('//a/@data-page')[0].extract())

	raw_json_data = pv_sel.xpath('//script[@id="ProductJson-nov-product-template"]/text()').extract()[0]
	json_data = json.loads(raw_json_data)
	product_id = json_data['id']
	all_reviews,review_dates = review_extractor(product_id,last_page)

	logger.info(f'Total reviews found: {str(len(all_reviews))}')
	return all_reviews,review_dates


@click.command()
@click.option("--filename", default='data', help="Name of the csv file")
@time_calculator
def lv_main(filename):
	"""
	The function lv_main() scrapes product information and reviews from a website and saves it to a CSV
	file.
	"""

	product_list = []

	try:
		url = 'https://www.scrollingbee.com/collections/raw-honey'
		sel = generate_selector(url)
		articles = sel.xpath('//div[@class="page-width"]//div[@data-section-id="collection-template"]//div[@class="row collection-view-items grid--view-items"]/div').extract()
		number_of_products = len(articles)
		logger.info(f"Found {number_of_products} Products:")

		for i,article in enumerate(articles,start=1):

			item = {}
			art_sel = Selector(text=article)

			try:
				product_img = 'https:'+art_sel.xpath('//div[@class="thumbnail-container"]//img/@src').extract()[0]
				
			except IndexError:
				product_img = 'https:'+art_sel.xpath('//div[@class="thumbnail-container has-multiimage"]//img/@src').extract()[0]

			product_name = art_sel.xpath('//div[@class="product__title"]//a/text()').extract()[0]
			product_url = "https://www.scrollingbee.com"+art_sel.xpath('//div[@class="product__title"]//a/@href').extract()[0]
			stars = art_sel.xpath('//div[@class="jdgm-prev-badge"]/span[@class="jdgm-prev-badge__stars"]/span').extract()
			count_stars = calculate_stars(stars)
			count_reviews = art_sel.xpath('//div[@class="jdgm-prev-badge"]//span[@class="jdgm-prev-badge__text"]//text()').extract()[0].strip().replace('reviews','').replace('review','').strip()
			product_price = art_sel.xpath('//div[@class="product__price"]//span[@class="product-price__price"]//text()').extract()[0].strip().replace('Rs. ','').replace(',','')

			logger.info(f'Fetching Reviews for {product_name}')
			all_reviews,review_dates = pv_main(product_url)

			item['NAME'] = product_name
			item['IMAGE'] = product_img
			item['URL'] = product_url
			item['PRICE'] = product_price
			item['RATING'] = count_stars
			item['NO_OF_REVIEWS'] = count_reviews
			item['REVIEWS'] = all_reviews
			item['DATES'] = review_dates
			
			product_list.append(item)

		logger.info(f"Fetched {len(product_list)} Items Successfully")
		logger.info('Proccessing CSV ...')
		create_csv(product_list,filename)
		logger.info('CSV File generated successfully')
		

	except IndexError as e:
		traceback_str = ''.join(traceback.format_tb(e.__traceback__))
		logger.error(f'Issue with product {i}:',traceback_str)
    
if __name__ == '__main__':
	lv_main()
	
        