import pandas as pd
import sys
import scrapydo
import scrapy

#from selenium import webdriver


def gsm_detail_crawler(ena_info):
    gsm_list = pd.read_csv(ena_info)['sample_alias'].to_list()
    # gsm_list = gsm_list[0:5]
    gsm_filename = ena_info.replace('ena_info.csv', 'gsm_info')
    #scRNA-seq databases
    class gsmItem(scrapy.Item):
        accession = scrapy.Field()
        title = scrapy.Field() #title
        source = scrapy.Field() # source
        organism = scrapy.Field() # organism:should be Homo Sapiens
        characteristics = scrapy.Field() # Characteristics of each GSE
        protocol = scrapy.Field() #contributors of each gse
        processing = scrapy.Field() # processing
        download_url = scrapy.Field() #download link
        download = scrapy.Field() #download filename
        gse_alias = scrapy.Field() #gse reference
        citation = scrapy.Field() # database reference
        # overall_design = scrapy.Field()
        
    class gsmSpider(scrapy.Spider):
        name = gsm_filename
        custom_settings = {
            'LOG_LEVEL': 'DEBUG',
            'ROBOTSTXT': False,
            'COOKIES_ENABLED': False,
            'DOWNLOAD_DELAY': 1.5,
            'DOWNLOADER_MIDDLEWARES' : {
                'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
                'scrapy_user_agents.middlewares.RandomUserAgentMiddleware': 400,
            },

            'FEEDS': {
                    '%(name)s.csv': {
                        'format': 'csv',
                    }
            }
        }
        page_index = 1

        def start_requests(self):
            gse_url = ('https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={}'.format(i) for i in gsm_list)
            for i in gse_url:
                item = gsmItem()
                item['accession'] = i[i.index('=')+1:]
                yield scrapy.Request(url=i,meta={'item':item},callback=self.parse_GSM,dont_filter=True) #GSE page

        def parse_GSM(self,response):
            # print('Now GSE is: '+response.url)
            # a scrapy item class for export
            item = response.meta['item']
            
            title = response.xpath('//td[contains(text(),"Title")]/following-sibling::node()/text()').extract()
            source = response.xpath('//td[contains(text(),"Source name")]/following-sibling::node()/text()').extract()
            organism = response.xpath('//td[contains(text(),"Organism")]/following-sibling::node()/a/text()').extract()
            characteristics = response.xpath('//td[contains(text(),"Characteristics")]/following-sibling::node()/text()').extract()
            protocol = response.xpath('//td[contains(text(),"Extraction protocol")]/following-sibling::node()/text()').extract()
            processing = response.xpath('//td[contains(text(),"processing")]/following-sibling::node()/text()').extract()
            # print(processing)
            gse_alias = response.xpath('//td/a[contains(text(),"GSE")]/text()').extract()
            # print(gse_alias)
            #backup: //td[contains(text(),"Citation(s)")/following-sibling::node()]
            ref = response.xpath('//span[@class="pubmed_id"]/a/text()').extract()
            #print(ref)
            pubmed = ['https://pubmed.ncbi.nlm.nih.gov/' + r for r in ref]

            item['title'] = title
            item['source'] = source
            item['characteristics'] = characteristics
            item['organism'] = [o for o in organism]
            item['processing'] = processing
            item['protocol'] = protocol
            item['gse_alias'] = gse_alias
    
            # item['contributors'] = [c for c in contributors]
            #print(wholeitem['contributors'])
            if ref:
                item['citation'] = pubmed
            yield item
            
            time.sleep(random.random()*3)
    # Time to initiate crawler
    scrapydo.setup()
    scrapydo.run_spider(gsmSpider)

input_gse = sys.argv[1]
ena_info = input_gse.replace('.txt', '_accession_ena_info.csv')
gsm_detail_crawler(ena_info)
