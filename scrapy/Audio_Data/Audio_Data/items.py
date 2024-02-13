# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class AudioDataItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    audio_names = scrapy.Field()
    actual_price = scrapy.Field()
    offer_price = scrapy.Field()
    reviews_count = scrapy.Field()

