import requests
from bs4 import BeautifulSoup
from urllib import parse
import re, time
from multiprocessing.managers import BaseManager

class HtmlDownloader:
    def download(self, url):
        header = {
            'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36',
        }
        try:
            html = requests.get(url, headers=header)
            # html = urlopen(url)
            if html.status_code != 200:
            # if html.getcode() != 200:
                return
            return html.content
            # return html.read()
        except:
            print('download failed')


class HtmlParser:
    def parse(self, url, html):
        soup = BeautifulSoup(html, 'html.parser')
        new_urls = self._get_new_urls(url, soup)
        data = self._get_data(url, soup)
        return new_urls, data

    def _get_new_urls(self, url, soup):
        new_urls = set()
        for link in soup('a', href=re.compile(r'^/item/.+')):
            new_urls.add(parse.urljoin(url, link['href']))
        return new_urls

    def _get_data(self, url, soup):
        data = {}
        title = soup.find(class_='lemmaWgt-lemmaTitle-title').find('h1').get_text()
        summary = soup.find(class_='lemma-summary').get_text()
        data['url'] = url
        data['title'] = title
        data['summary'] = summary
        print('%s, %s, %s' % (url, title, summary))
        return data

class Spider:
    def __init__(self):
        BaseManager.register('get_task_queue')
        BaseManager.register('get_result_queue')
        server_addr = '127.0.0.1'
        print('连接到服务器%s...' % server_addr)
        self.m = BaseManager(address=(server_addr, 8001), authkey=b'baike')
        self.m.connect()
        self.task = self.m.get_task_queue()
        self.result = self.m.get_result_queue()
        print(self.task, self.result)
        self.downloader = HtmlDownloader()
        self.parser = HtmlParser()
        print('爬虫节点初始化完成!')

    def crawl(self):
        while True:
            try:
                if not self.task.empty():
                    url = self.task.get()
                    if url == 'end':
                        print('爬虫节点收到结束！')
                        self.result.put({'new_urls': 'end', 'data': 'end'})
                        return
                    print('爬虫节点正在解析:%s'%url)
                    html = self.downloader.download(url)
                    new_urls, data = self.parser.parse(url, html)
                    self.result.put({'new_urls': new_urls, 'data': data})
                else:
                    time.sleep(0.1)
            except:
                time.sleep(0.1)

if __name__=="__main__":
    spider = Spider()
    spider.crawl()