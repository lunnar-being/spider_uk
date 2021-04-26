import json
import re
from datetime import timedelta
from pprint import pprint

import urllib.parse as url_parser
from lxml import etree
import requests_cache
import fake_useragent

from download import Downloader
from redis_queue import RedisQueue
from app.models import PolicyText, File
from app import db

fake_ua = fake_useragent.UserAgent()  # 设置 useragent
keywords_list = [
    "disruptive technology",
    "disruptive innovation",
    "emerging technology",
    "discontinuous technology",
    "developing technology",
    "advanced technology",
    "integrated technology",
    "future technology",
    "promising technology",
    "next generation technology",
    "evolving technology",
    "radical technology",
    "diffusion of innovation",
    "technology diffusion",
    "national innovation system",
    "enabling technology"
]  # 检索关键词

# 获取任务队列, 数据库为第1号
crawl_queue = RedisQueue(db=1)
# 建立redis缓存, 缓存保存时间设置为3天
requests_cache.install_cache(backend='redis', expires=timedelta(days=3))


def search_crawler(url, xpath):
    """
    解析一个检索结果页面，获取政策链接
    Args:
        url (str):      检索结果页面链接
        xpath (str):    每一条结果的URL对应的XPATH
    """
    downloader = Downloader(delay=3, user_agent=fake_ua['random'])
    html = downloader(url, num_retries=3)
    dom = etree.HTML(html)
    page_url_list = dom.xpath(xpath)
    return page_url_list


def link_crawler(delay=3, callback=None):
    """
    爬取政策
    Args:
        delay (int):            延迟秒数 (default: 3)
        callback (function):    对下载内容进行处理的回调函数
    """
    downloader = Downloader(delay=delay, user_agent=fake_ua['random'])
    # 不断循环任务队列
    # todo 有个小问题，目前的逻辑是先把任务链接全爬下来，如果以后一边加任务一边执行任务，可能会因为任务断了而结束
    while len(crawl_queue):
        url = crawl_queue.pop()
        html = downloader(url, num_retries=3)
        if callback:
            callback(html, url)


def test_link_crawler(callback=None):
    """用于测试数据抓取"""
    url = 'https://op.europa.eu/en/publication-detail/-/publication/17736205-7654-11eb-9ac9-01aa75ed71a1/language-en/format-PDF/source-198932761'
    downloader = Downloader(delay=3, user_agent=fake_ua['random'])
    html = downloader(url, num_retries=3)
    callback(html, url)


def gen_tasks():
    """获取所有政策链接"""
    start_row = 1
    query_url = 'https://op.europa.eu/en/search-results?p_p_id=eu_europa_publications_portlet_search_executor_SearchExecutorPortlet_INSTANCE_q8EzsBteHybf&p_p_lifecycle=1&p_p_state=normal&queryText={}&facet.collection=EUPub&startRow=1&resultsPerPage=50&SEARCH_TYPE=SIMPLE&startRow={}'
    for query_word in keywords_list:
        print(f'[INFO] Start Query Keyword: {query_word}')

        # 以下用于获取结果条数，用于翻页
        len_info = search_crawler(url=query_url.format(query_word, start_row),
                                  xpath="//span[@class='results-number-info']/text()")
        len_re = re.match(r"returned\s*(\d+)\s*results", len_info[0].strip())
        tot_len = int(len_re.group(1))
        print(f'[INFO] Query Result Len: {tot_len}')

        while start_row < tot_len:
            page_list = search_crawler(
                url=query_url.format(query_word, start_row),
                xpath='//a[@class="documentDetailLink"]/@href')
            print(
                f'[DONE] start_row: {start_row}, {len(page_list)} page_list got.')
            crawl_queue.push(page_list)
            start_row += 50


def handle_policy_op_europa_eu(html, url):
    def strip_list(ele_list):
        """用来处理xpath抓取结果"""
        return [ele.strip() for ele in ele_list if ele]

    # 定义一堆xpath
    xpath_dict = {
        'publish_time': "//time[@itemprop='datePublished']/text()",
        'author': "//li[@class='list-item last']//a/text()",
        'themes': "//li[@class='list-item list-item-themes']//a[1]/text()",
        'keywords': "//li[contains(@class,'list-item last list-item-subject')]/a/text()",
        'file_url': "//a[@data-format='pdf']/@data-uri",
        'description': "//div[@itemprop='description']//span/text()",
        'bread': "//ol[@class='breadcrumb']/li//span/text()",
        'title': "//h1[@class='main-publication-title']/text()"
    }

    dom = etree.HTML(html)

    policy_text = PolicyText()
    policy_text.source_url = url
    policy_text.nation = 'EU'
    policy_text.release_time = dom.xpath(xpath_dict['publish_time'])[0].strip()
    policy_text.language = 'EN'
    policy_text.institution = json.dumps(strip_list(
        dom.xpath(xpath_dict['author'])))
    policy_text.field = json.dumps({
        'themes': strip_list(dom.xpath(xpath_dict['themes'])),
        'bread': strip_list(dom.xpath(xpath_dict['bread']))
        # fixme 爬取的网页没有第三个面包屑
    })
    policy_text.original_title = dom.xpath(xpath_dict['title'])[0].strip()
    policy_text.description = dom.xpath(xpath_dict['description'])[0].strip()

    file = File()
    file.name = policy_text.original_title
    file.extension = 'PDF'
    file.size = '0'
    file.source_url = dom.xpath(xpath_dict['file_url'])[0].strip()
    file.source_url = url_parser.urljoin(url, file.source_url)
    db.session.add(file)
    db.session.commit()

    policy_text.original_file = file.id
    db.session.add(policy_text)
    db.session.commit()


if __name__ == '__main__':
    gen_tasks()
    # link_crawler(callback=handle_policy_op_europa_eu)
    # test_link_crawler(callback=handle_policy_op_europa_eu)
