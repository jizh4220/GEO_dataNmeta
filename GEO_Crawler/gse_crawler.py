import scrapy
import scrapydo
import glob
import pandas as pd
import sys
from datetime import date


#from selenium import webdriver


def crawler_wrapper(input_gse):
    df = pd.read_table(input_gse, header=None)
    df[0] = df[0].astype(str).replace('^200', 'GSE', regex=True)
    df[0] = df[0].replace('^30', 'GSM', regex=True)
    gse_list = df[df[0].str.contains('GSE')]
    gse_list = gse_list[0].tolist()
    gse_filename = input_gse.replace('.txt', '_detail_info')

    #scRNA-seq databases
    class gseItem(scrapy.Item):
        gse_alias = scrapy.Field()
        date = scrapy.Field() #published/updated date
        title = scrapy.Field() # title
        organism = scrapy.Field() # organism:should be Homo Sapiens
        summary = scrapy.Field() # summary of each GSE
        contributors = scrapy.Field() #contributors of each gse
        download_url = scrapy.Field() #download link
        download = scrapy.Field() #download filename
        bioproject = scrapy.Field()
        #gsm = scrapy.Field() #gsm download url
        citation = scrapy.Field() # database reference
        overall_design = scrapy.Field()
        
    class gseSpider(scrapy.Spider):
        name = gse_filename
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
            gse_url = ('https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={}'.format(i) for i in gse_list)
            for i in gse_url:
                item = gseItem()
                item['gse_alias'] = i[i.index('=')+1:]
                yield scrapy.Request(url=i,meta={'item':item},callback=self.parse_GSE,dont_filter=True) #GSE page

        def parse_GSE(self,response):
            # print('Now GSE is: '+response.url)
            # a scrapy item class for export
            item = response.meta['item']
            
            date = response.xpath('//td[contains(text(),"Status")]/following-sibling::node()/text()').extract()
            title = response.xpath('//td[contains(text(),"Title")]/following-sibling::node()/text()').extract()
            organism = response.xpath('//td[contains(text(),"Organism")]/following-sibling::node()/a/text()').extract()
            summary = response.xpath('//td[contains(text(),"Summary")]/following-sibling::node()/text()').extract()
            overall = response.xpath('//td[contains(text(),"Overall design")]/following-sibling::node()/text()').extract()
            contributors = response.xpath('//td[contains(text(),"Contributor(s)")]/following-sibling::node()/a/text()').extract()
            bioproject = response.xpath('//td[contains(text(),"BioProject")]/following-sibling::node()/a/text()').extract()
            #backup: //td[contains(text(),"Citation(s)")/following-sibling::node()]
            ref = response.xpath('//span[@class="pubmed_id"]/a/text()').extract()
            #print(ref)
            pubmed = ['https://pubmed.ncbi.nlm.nih.gov/' + r for r in ref]
            accession_file = item['gse_alias']+'_'
            download = response.xpath('//td[contains(text(),"'+accession_file+'")]/text()').extract()
            #print(download)
            download_url = response.xpath('//a[contains(text(),"(ftp)")]/@href').extract()
            http_url = response.xpath('//a[contains(text(),"(http)")]/@href').extract()
            if not download_url:
                item['download_url'] = ['https://www.ncbi.nlm.nih.gov' + h for h in http_url]
            else:   
                item['download_url'] = [s for s in download_url]
                
            item['download'] = [d for d in download]
            

            item['title'] = title
            item['date'] = date
            item['overall_design'] = overall
            item['organism'] = [o for o in organism]
            item['bioproject'] = bioproject
            item['summary'] = summary
    
            item['contributors'] = [c for c in contributors]
            #print(wholeitem['contributors'])
            if ref:
                item['citation'] = pubmed
            yield item
            
            time.sleep(random.random()*3)
    # Time to initiate crawler
    scrapydo.setup()
    scrapydo.run_spider(gseSpider)
    

#  input_gse = 'need_download/Need_Download_*.txt'
input_gse = sys.argv[1]
crawler_wrapper(input_gse)

# accessioninfo = input_gse.replace('.txt', '_detail_info.csv')
# test = pd.read_csv(accessioninfo)
# non_humanlung = 'media|cell line|organoid|derived|MRC5|NT2-D1|2102EP|NCCIT|TCAM2|HUVEC|H441|BOEC|HUDEP2|A20+K562|H1299|Calu3|MDA-|culture|A549|brain|SW1573|hESC|h358|LM2|organotypic|Transplants|CKDCI|IMR90|IR Senescence|Nkx2.1+ RUES2|AALE|HCT116|BEAS-2B|RT4|LNCaP|H1|HBEC1|HBEC2|Lib_CGA|A549|MDCK|A427|Calu3|LNCaP/AR|AT2|iAT2|ALT-43T|HuT102|MJ|MT-4C16DMS53H524H841H69H82H1048CORL279DMS454H1299NCI-H3122 H3122NCI-H358H358SW480H1299Calu-3LC2PROCL1-5PC9CL1-0Cyt49LC-2/adH2009del-VSMC16HBE14o-prostateH7ECAD9'

# f_test = test[~test['summary'].str.contains(non_humanlung, case=False)]
# f_test = f_test[~f_test['title'].str.contains(non_humanlung, case=False)]

