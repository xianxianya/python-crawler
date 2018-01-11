# -*- coding: utf-8 -*-
import queue, time
from multiprocessing.managers import BaseManager
from multiprocessing import Process, Queue


class UrlManager:
    ORIGINAL = 0  # 初始状态
    SUCCESS = 1  # 已完成
    FAILED = 2  # 爬取失败或超时
    PROCCESSING = 3  # 正在爬取

    def __init__(self):
        self.new_urls = set()
        self.old_urls = set()

    def add_url(self, url):
        if url is None:
            return
        if url not in self.new_urls and url not in self.old_urls:
            self.new_urls.add(url)

    def add_urls(self, urls):
        if urls is None or len(urls) == 0:
            return
        for url in urls:
            self.add_url(url)

    def has_url(self):
        return len(self.new_urls) > 0

    def get_url(self):
        if not self.has_url():
            return
        url = self.new_urls.pop()
        self.old_urls.add(url)
        return url

    def get_old_url_size(self):
        return len(self.old_urls)

class Outputer:
    def __init__(self):
        self.data = []

    def collect_data(self, data):
        if data is None:
            return
        self.data.append(data)

    def output_html(self):
        with open('/Users/yaoyuan/Downloads/baike.html', 'w', encoding='utf-8') as f:
            f.write('<html>')
            f.write("<meta charset='utf-8'>")
            f.write('<body>')
            f.write('<table>')
            for data in self.data:
                f.write('<tr>')
                f.write("<td>%s</td>" % data['title'])
                f.write("<td>%s</td>" % data['summary'])
                f.write('</tr>')
            f.write('</table>')
            f.write('</body>')
            f.write('</html>')


class Scheduler:
    def __init__(self):
        pass

    def start(self, url_q, result_q):
        BaseManager.register('get_task_queue', callable=lambda: url_q)
        BaseManager.register('get_result_queue', callable=lambda: result_q)
        manager = BaseManager(address=('127.0.0.1', 8001), authkey=b'baike')
        return manager


    def url_manager_proc(self, url_q, conn_q, root_url):
        url_manager = UrlManager()
        url_manager.add_url(root_url)
        while True:
            while url_manager.has_url():
                new_url = url_manager.get_url()
                url_q.put(new_url)
                if url_manager.get_old_url_size()>1000:
                    url_q.put('end')
                    print('控制节点发起结束通知！')
                    return
            try:
                if not conn_q.empty():
                    url_manager.add_urls(conn_q.get())
            except:
                time.sleep(0.1)


    def result_manager_proc(self, result_q, conn_q, store_q):
        while True:
            try:
                if not result_q.empty():
                    res = result_q.get()
                    if res['new_urls'] == 'end':
                        print('结果分析进程收到结束！')
                        store_q.put(res['data'])
                        return
                    conn_q.put(res['new_urls'])
                    store_q.put(res['data'])
                else:
                    time.sleep(0.1)
            except:
                time.sleep(0.1)


    def store_manager_proc(self, store_q):
        output = Outputer()
        while True:
            try:
                if not store_q.empty():
                    data = store_q.get()
                    if data == 'end':
                        print('存储进程收到结束！')
                        output.output_html()
                        return
                    output.collect_data(data)
                else:
                    time.sleep(0.1)
            except:
                time.sleep(0.1)


if __name__ == '__main__':
    root_url = 'https://baike.baidu.com/item/Python/407313'
    scheduler = Scheduler()
    # 普通的queue.Queue
    url_q = queue.Queue()  # 发送给爬虫的url队列
    result_q = queue.Queue()  # 爬虫发送给调度器的结果队列
    # 进程间的通信multiprocessing.Queue！！！
    conn_q = Queue()  # 数据提取进程发送给url管理进程的url队列
    store_q = Queue()  # 数据提取进程发送给数据存储进程的数据队列
    manager = scheduler.start(url_q, result_q)
    # manager start之后调用注册后的队列，才会在网络间传递数据！！！
    manager.start()
    url_q = manager.get_task_queue()
    result_q = manager.get_result_queue()
    url_manager_proc = Process(target=scheduler.url_manager_proc, args=(url_q, conn_q, root_url))
    result_solve_proc = Process(target=scheduler.result_manager_proc, args=(result_q, conn_q, store_q))
    store_proc = Process(target=scheduler.store_manager_proc, args=(store_q,))
    url_manager_proc.start()
    result_solve_proc.start()
    store_proc.start()
    # 需要join，不然主函数执行完毕直接关闭报错！！！
    url_manager_proc.join()
    result_solve_proc.join()
    store_proc.join()
    # 关闭，不然函数可能不结束！！！
    manager.shutdown()
    # manager.get_server().serve_forever()