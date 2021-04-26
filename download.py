from random import choice
import requests
import requests_cache

from urllib.parse import urlparse
import time


class Throttle:
    """ 控制访问间隔 """

    def __init__(self, delay):
        # 同一个域名的间隔
        self.delay = delay
        # 记录域名上次访问时间
        self.domains = {}

    def wait(self, url):
        domain = urlparse(url).netloc
        last_accessed = self.domains.get(domain)

        if self.delay > 0 and last_accessed is not None:
            sleep_secs = self.delay - (time.time() - last_accessed)
            if sleep_secs > 0:
                # 最近访问过，需要休息
                time.sleep(sleep_secs)
        # 更新访问时间
        self.domains[domain] = time.time()


class Downloader:
    """
    使用 Cache 和 Requests 的下载器
    Args:
        delay (int):            间隔的秒数 (default: 5)
        user_agent (str):       UA
        proxies (list[dict]):   http / https 开头的键值对
        timeout (float/int):    超时
    """

    def __init__(self, user_agent, delay=5, proxies=None, timeout=60):
        self.throttle = Throttle(delay)
        self.user_agent = user_agent
        self.proxies = proxies
        self.num_retries = None
        self.timeout = timeout

    def __call__(self, url, num_retries=2):
        """
        调用下载功能，爬取或者使用缓存
        Args:
            url (str):          下载的链接
        Keyword Args:
            num_retries (int):  如果5xx再尝试次数 (default: 2)
        """
        self.num_retries = num_retries
        proxies = choice(self.proxies) if self.proxies else None
        headers = {'User-Agent': self.user_agent}
        result = self.download(url, headers, proxies)
        return result['html']

    @staticmethod
    def make_throttle_hook(throttle=None):
        """
        resp 勾子函数 可以判断是否使用了缓存
        """
        def hook(response, *args, **kwargs):
            if not getattr(response, 'from_cache', False):
                throttle.wait(response.url)
                print('Downloading:', response.url)
            else:
                print('Returning from cache:', response.url)
            return response
        return hook

    def download(self, url, headers, proxies):
        """
        Download a and return the page content

        Args:
            url (str):          URL
            headers (dict):     请求头
            proxies (dict):     代理字典
                                keys 'http'/'https' values 'http(s)://IP'
        """
        # session = requests_cache.CachedSession(backend='redis')
        session = requests.session()
        session.hooks = {'response': self.make_throttle_hook(self.throttle)}

        try:
            resp = session.get(url, headers=headers, proxies=proxies,
                               timeout=self.timeout)
            html = resp.text
            if resp.status_code >= 400:
                print('Download error:', resp.text)
                html = None
                if self.num_retries and 500 <= resp.status_code < 600:
                    # recursively retry 5xx HTTP errors
                    self.num_retries -= 1
                    return self.download(url, headers, proxies)
        except requests.exceptions.RequestException as e:
            print('Download error:', e)
            return {'html': None, 'code': 500}
        return {'html': html, 'code': resp.status_code}
