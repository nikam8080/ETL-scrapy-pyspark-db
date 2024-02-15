from random import randrange
import scrapy
from ..items import AudioDataItem
# from datetime import data, datetime, timedelta
# sfrom Audio_Data.Audio_Data.items import AudioDataItem

class FlipkartScrawling(scrapy.Spider):
    name = 'flipkart'

    page_number = 2

    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.9999.99 Safari/537.36',
    }

    # start_urls = [
    #     'https://www.flipkart.com/headset/pr?sid=fcn&otracker=categorytree&page=1'
    # ]

    start_urls = [
        'https://www.flipkart.com/search?q=audio&otracker=search&otracker1=search&marketplace=FLIPKART&as-show=on&as=off&page=1'
    ]

    def parse(self, response):
        items = AudioDataItem()

        audio_names = response.css("a.s1Q9rs::text").extract()
        actual_price = response.css("div[class=_3I9_wc]::text").extract()
        offer_price = response.css("div[class=_30jeq3]::text").extract()
        # discount = response.css("span[class='_1lRcqv']::text").extract()
        reviews_count = response.css("[class=_2_R_DZ]::text").extract()
        audio_type = response.css("[class=_3Djpdu]::text").extract()

        formatted_actual_price = []

        for i in range(0, len(actual_price), 2):
            formatted_actual_price.append(actual_price[i] + actual_price[i + 1])


        for item in zip(audio_names,formatted_actual_price, offer_price, reviews_count, audio_type):
            scrapped_info = {
                'audio_name': item[0],
                'formatted_actual_price': item[1],
                'offer_price': item[2],
                # 'rating': item[3],
                'reviews_count': item[3],
                'audio_type': item[4]        
            }
            
            yield scrapped_info

        items['audio_names'] = audio_names
        items['actual_price'] = formatted_actual_price
        items['offer_price'] = offer_price
        # items['discount'] = discount
        items['reviews_count'] = reviews_count
        items['audio_type'] = audio_type

        yield items

        # next_page = "https://www.flipkart.com/headset/pr?sid=fcn&otracker=categorytree&page=1"+str(FlipkartScrawling.page_number)
        # if FlipkartScrawling.page_number <= 3:
        #     FlipkartScrawling.page_number += 1
        #     yield response.follow(next_page, callback = self.parse)

        next_page = "https://www.flipkart.com/search?q=audio&otracker=search&otracker1=search&marketplace=FLIPKART&as-show=on&as=off&page=1"+str(FlipkartScrawling.page_number)
        if FlipkartScrawling.page_number <= 50:
            FlipkartScrawling.page_number += 1
            yield response.follow(next_page, callback = self.parse)