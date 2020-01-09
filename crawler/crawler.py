import os
import re

from collections import deque
import requests
from bs4 import BeautifulSoup
import json
import time
from contextlib import contextmanager
from copy import deepcopy
import logging
import pickle

class DataType:
    __slot__ = []
    def __init__(self,**kwargs):
        [self.__setattr__(key,kwargs.get(key) if kwargs else None) for key in self.__slot__]

    def to_dict(self,**kwargs):
        dic = deepcopy(self.__dict__)
        if kwargs:
            dic.update(kwargs)
        return dic

class CreateDataType:
    def __init__(self,cls):
        self.data = cls()

    def add_attr(self,key,value):
        setattr(self.data,key,value)

    def __call__(self):
        return self.data
class CreateDataType:
    def __init__(self,cls):
        self.data = cls()

    def add_attr(self,key,value):
        setattr(self.data,key,value)

    def __call__(self):
        return self.data

class Engine:
    def __init__(self):
        self.response = deque()
        self.html_element = deque()
        self.content = deque()
    
    def build_session(self):
        self.session = requests.Session()

    def close_session(self):
        self.session.close()

    def reset(self):
        self.response = deque()
        self.html_element = deque()
        self.content = deque()

################# get requests ####################

def create_request(url, session, **kwargs):
    try:
        req = session.get(url, verify=False, allow_redirects=True, **kwargs)
        req.raise_for_status()
        return req
    except Exception as e:
        logger.error(e)
        pass

################# positioning #####################
def create_response(req,selector=None):
    page = BeautifulSoup(req.content,'html.parser')
    if not selector:
        return [page]
    else:
        return page.select(selector)

def run_downloader(response_q,req,selector):
    resp = create_response(req,selector)
    for response in resp:
        response_q.append(response)
    return resp

################## get html element ##################

def consume_message_q(queue):
    while queue:
        item = queue.pop()
        yield item

@contextmanager
def fetch_element(func,*args,**kwargs):
    if not args or args[0] is None:
        yield None
    else:
        try:
            yield func(*args,**kwargs)
        except Exception as e:
            print(e)
            yield None

def select_elements(response,selector=None):
    if not selector:
        return response
    return response.select(selector)[0]

def create_html_element(response,cls,**kwargs):
    create_data = CreateDataType(cls)
    for key in kwargs.keys():
        with fetch_element(select_elements,response,selector=kwargs.get(key)) as element:
            create_data.add_attr(key,element)
    return create_data()

def run_spider(response_q,element_q,cls,**kwargs):
    for response in consume_message_q(response_q):
        html_element = create_html_element(response,cls,**kwargs)
        element_q.append(html_element)

##################### parse content #####################

def extract_content(html_element,**kwargs):
    attr = kwargs.get('types')
    parser = kwargs.get('parser')
    if not attr or not parser:
        return
    elif attr == 'text':
        content = html_element.text
    else:
        content = html_element.get(attr)
    return parser(content)

def create_content(html_element,cls,**kwargs):
    if not isinstance(html_element,cls):
        raise TypeError('not element!')
    create_data = CreateDataType(cls)
    for key in kwargs.keys():
        with fetch_element(extract_content,getattr(html_element,key),**kwargs.get(key)) as content:
            create_data.add_attr(key,content)
    return create_data()

def run_parser(element_q,content_q,cls,**kwargs):
    for html_element in consume_message_q(element_q):
        content = create_content(html_element,cls,**kwargs)
        #print(content.__dict__)
        content_q.append(content)

def decode_content(func,content_q,*args,**kwargs):
    data = []
    for content in consume_message_q(content_q):
        item = func(content,*args,**kwargs)
        data.append(item)
    return data

###################################################################

class CrawlerBuilder:
    def __init__(self):
        self.engine = Engine()

    def configure(self,conf):
        self.request_conf = conf.request
        self.downloader_conf = conf.downloader
        self.spider_conf = conf.spider
        self.parse_conf = conf.parser

    def add_configure(self,**kwargs):
        [self.__setattr__('{}_conf'.format(key),value) for key,value in kwargs.items()]

    def start(self):
        self.engine.build_session()

    def create_request(self, url):
        return create_request(url, self.engine.session, **self.request_conf)

    def run_downloader(self,req,selector=None):
        if not selector:
            selector = self.downloader_conf
        return run_downloader(self.engine.response,req,selector)

    def run_spider(self,cls,**kwargs):
        if not kwargs:
            kwargs = self.spider_conf
        run_spider(self.engine.response,self.engine.html_element,cls,**kwargs)

    def run_parser(self,cls,**kwargs):
        if not kwargs:
            kwargs = self.parse_conf
        run_parser(self.engine.html_element,self.engine.content,cls,**kwargs)

    def decode_content(self,func,*args,**kwargs):
        """ give q a name """
        return decode_content(func,self.engine.content,*args,**kwargs)

    def close(self):
        self.engine.close_session()
        self.engine.reset()

    def run(self,url,spider_types,parser_types):
        req = self.create_request(url)
        self.run_downloader(req)
        self.run_spider(spider_types)
        self.run_parser(parser_types)
        return self.engine.content

    def __call__(self,start_url_list,step=10,time_sleep=30,spider_types=UrlType,parser_types=UrlType):
        if isinstance(start_url_list,str):
            self.run(start_url_list,spider_types,parser_types)
            return self.engine.content
        for s,url in enumerate(start_url_list):
            if s > 0 and s%step == 0:
                time.sleep(time_sleep)
            if not url:
                continue
            self.run(url,spider_types,parser_types)
        return self.engine.content

########################### I/O ##############################

def readS3(bucket,key):
    s3 = boto3.client('s3')
    response = boto3.get_object(Bucket=bucket,Key=key)
    data = response['Body'].read()
    return pickle.loads(data)

def writeS3(bucket,key,data):
    _data = pickle.dumps(data)
    s3 = boto3.client('s3')
    response = s3.upload_file(_data,bucket,key)
    return

def to_dict(self,q,k,vals):
        data = dict()
        while q:
            item = q.pop()
            key = getattr(item,k)
            if isinstance(vals,str):
                value = getattr(item,vals)
            if isinstance(vals,list):
                value = (getattr(item,val) for val in vals)
            if key and key not in data:
                data[key] = value
        return data


def get_domain(url):
    return url.replace("https://","").replace("http://","").replace("HTTPS://","").replace("HTTP://","").replace("www.","").split('/')[0].replace('?','')

############################################################
def switch_page(resp,**kwargs):
    with fetch_element(select_elements,resp,selector=kwargs.get('switch_selector')) as html_element:
        with fetch_element(extract_content,html_element,**kwargs.get('switch_parser')) as url:
            return url

class BloomNationCrawlerBuilder(CrawlerBuilder):
    def __init__(self,**kwargs):
        self.add_configure(**kwargs)
        CrawlerBuilder.__init__(self)

    def fetch_all_pages(self,req):
        kwargs = {
            'switch_selector':self.switch_selector_conf,
            'switch_parser' : self.switch_parser_conf
        }
        dq = deque()
        resp = run_downloader(dq,req,selector=None)
        while True:
            self.run_downloader(req, selector=self.shop_downloader_conf)
            url = switch_page(resp[0],**kwargs)
            if not url:
                break
            print('next-page-link:  {}'.format(url))
            req = self.create_request(url)
            resp = run_downloader(dq,req,selector=None)
        del dq

    def download_shop_url(self,start_url):
        req = self.create_request(start_url)
        self.fetch_all_pages(req)
        self.run_spider(cls=UrlType,**self.shop_spider_conf)
        self.run_parser(cls=UrlType,**self.shop_parser_conf)
        shop_urls = set(self.decode_content(lambda x:x.link if x else None))
        shop_urls = list(filter(lambda x:x and x.startswith('https://www.bloomnation.com'),shop_urls))
        self.engine.reset()
        return shop_urls

    def __call__(self,start_url_list,spider_types,parser_types,step=10,time_sleep=30):
        _start_url_list = [start_url_list] if isinstance(start_url_list,str) else start_url_list
        for s,url in enumerate(_start_url_list):
            if s > 0 and s%step == 0:
                time.sleep(time_sleep)
            shop_urls = self.download_shop_url(url)
            print(shop_urls)
            CrawlerBuilder.__call__(self,shop_urls,step=step,time_sleep=time_sleep,spider_types=spider_types,parser_types=parser_types)
        return self.engine.content

###################### main FUNC ##################################
@contextmanager
def open_crawler(cls,conf,**kwargs):
    crawler = cls(**kwargs)
    crawler.configure(conf)
    crawler.start()
    yield crawler
    crawler.close()