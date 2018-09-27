import os
import datetime
import shutil
import http
import argparse
import numpy as np
import requests
import pprint
import queue
import urllib.request
from html.parser import HTMLParser
from urllib.parse import urlparse
import urllib.robotparser
import heapq
import socket


class MyHTMLParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links = []
        self.lines = []
        self.base = ''
        
    def handle_starttag(self, tag, attrs):
        for name, value in attrs:
            # blacklist of file ending
            if name == 'href' and value and not value.split('.')[-1] in ['css', 'jpg', 'png', 'ico', 'mp3', 'mp4', 'pdf', 'cgi']:
                if tag == 'base':
                    self.base = value
                    continue
                o = urlparse(value)
                # TODO: relative url
                if o.scheme in ['http', 'https']:
                    self.links.append(value)
                
    def handle_endtag(self, tag):
        pass

    def handle_data(self, data):
        # exclude javascript
        if data.strip() and '{' not in data:
            self.lines.append(data.strip())


def parse_page(filename):
    with open(filename, 'r', encoding = 'UTF-8', errors = 'ignore') as f:
        size = os.path.getsize(filename)
        parser = MyHTMLParser()
        parser.feed(f.read())
    return parser.lines, list(set(parser.links)), size


# robot exclusion protocol
def can_visit(url):
    o = urlparse(url)
    rb_url = o.scheme + '://' + o.netloc + '/robots.txt'
    rp = urllib.robotparser.RobotFileParser()
    rp.set_url(rb_url)
    try:
        # might denied
        rp.read()
        return rp.can_fetch('*', url)
    except:
        return False


# ralevance function
def relevance(query, lines):
    query_ws = query.lower().split()
    cnt = 0
    for line in lines:
        for w in line.split():
            if w.lower() in query_ws:
                cnt += 1
    # consider multiple keywords
    return cnt / len(query_ws)


def url_score(query, url):
    query_ws = query.lower().split()
    url_score = 0
    for w in query_ws:
        if len(w) > 3:
            # substring occurence, because plural nonus might be used
            for j in range(3, len(w) + 1):
                if w[:j] in url.lower():
                    url_score += 1
        else:
            if w in url.lower():
                    url_score += 1
    if url_score > len(query_ws):
        return url_score
    else:
        return 0


# detection pre-visited
def normalize_url(url):
    l = url.split('//')
    return ''.join(l[1:])


def check_header(headers):
    for k in headers.keys():
        if k.lower() == 'content-type' and 'text/html' in headers[k]:
            return True
    return False


class Crawler():
    def __init__(self):
        self.log = []
        self.parsed_num = 0
        self.num_404 = 0
        self.time = 0
        self.rel_num = 0


    def reset(self):
        self.log = []
        self.parsed_num = 0
        self.num_404 = 0
        self.time = 0
        self.rel_num = 0


    def savelog(self, filename):
        with open(filename, 'w') as f:
            for l in self.log:
                f.write(','.join(str(e) for e in l) + '\n')
            f.write('total number of files: ' + str(len(self.log)) + '\n')
            f.write('total size: ' + str(sum([e[2] for e in self.log])) + '\n')
            f.write('number of 404 errors: ' + str(self.num_404) + '\n')
            f.write('harvest rate: ' + str(self.rel_num / len(self.log)) + '\n')
            f.write('total time: ' + str(self.time) + '\n')


    def bfs(self, query, MAX_NUM, MAX_PER_SITE):
        self.reset()
        start = datetime.datetime.now()
        if os.path.exists('bfs_crawled'):
            shutil.rmtree('bfs_crawled')
        os.mkdir('bfs_crawled')
        Q = queue.Queue()
        visited = {}
        site_num = {}
        for result in results:
            Q.put(
                {
                    'url': result['url'],
                    'depth': 0,
                }
            )
            url = normalize_url(result['url'])
            visited[url] = 0
            site_num[url.split('/')[0]] = 1

        while self.parsed_num < MAX_NUM:
            # robot exclusion
            cur = Q.get()
            print('*' * 10)
            cur_url = cur['url']
            print(cur_url)
            if not can_visit(cur_url):
                continue
            # retrieve file
            try:
                local_filename, headers = urllib.request.urlretrieve(cur_url, filename = 'bfs_crawled/file' + str(self.parsed_num) +'.txt')
            # handle Forbidden error
            except urllib.error.HTTPError as err:
                print(err)
                if '404' in str(err):
                    self.num_404 += 1
            # avoid timeout
            except socket.timeout:
                count = 1
                while count <= 5:
                    try:
                        local_filename, headers = urllib.request.urlretrieve(cur_url, filename = 'bfs_crawled/file' + str(self.parsed_num) +'.txt')
                        break
                    except socket.timeout:
                        err_info = 'Reloading for %d time'%count if count == 1 else 'Reloading for %d times'%count
                        print(err_info)
                        count += 1
                if count > 5:
                    print('downloading failed!')
            except (UnicodeEncodeError, ConnectionResetError, http.client.HTTPException, urllib.error.URLError) as err:
                print(err)
            else:
                if not check_header(headers):
                    continue
                print('downloaded')
                print('#', self.parsed_num)
                time = str(datetime.datetime.now())
                print('time', time)
                lines, urls, size = parse_page(local_filename)
                print('size', size)
                # calculate the relevance
                cur_score = relevance(query, lines)
                print('relevance', cur_score)
                if cur_score >= 1:
                    self.rel_num += 1
                # record the page
                self.parsed_num += 1
                self.log.append(
                    (
                        cur_url, time, size, '200', 'bfs', cur_score,
                    )
                )
                # BFS
                for url in urls:
                    if normalize_url(url) in visited:
                        # plain
                        continue
                    n_url = normalize_url(url)                    
                    if n_url.split('/')[0] not in site_num or site_num[n_url.split('/')[0]] < MAX_PER_SITE:
                        Q.put(
                            {
                                'url': url,
                                'depth': cur['depth'] + 1,
                            }
                        )
                        visited[n_url] = cur['depth'] + 1
                        site_num[n_url.split('/')[0]] = site_num.get(n_url.split('/')[0], 0) + 1
        end = datetime.datetime.now()
        self.time = end - start
                    
    
    def focus(self, query, MAX_NUM, MAX_PER_SITE):
        self.reset()
        start = datetime.datetime.now()
        if os.path.exists('focus_crawled'):
            shutil.rmtree('focus_crawled')
        os.mkdir('focus_crawled')
        # initial the queue
        heap = []
        visited = {}
        site_num = {}
        for result in results:
            heapq.heappush(heap, (0, result['url']))
            # normalize url
            url = normalize_url(result['url'])
            visited[url] = 0
            site_num[url.split('/')[0]] = 1
            
        level = 0
        while self.parsed_num < MAX_NUM:
            d_scores = {}
            while len(heap):
                prior_score, cur_url = heapq.heappop(heap)
                print('*' * 10)
                print(cur_url)
                # robot exclusion
                if not can_visit(cur_url):
                    continue
                # retrieve file
                try:
                    local_filename, headers = urllib.request.urlretrieve(cur_url, filename = 'focus_crawled/file' + str(self.parsed_num) + '.txt')
                # handle Forbidden error
                except urllib.error.HTTPError as err:
                    print(err)
                    if '404' in str(err):
                        self.num_404 += 1
                except socket.timeout:
                    count = 1
                    while count <= 5:
                        try:
                            local_filename, headers = urllib.request.urlretrieve(cur_url, filename = 'bfs_crawled/file' + str(self.parsed_num) +'.txt')
                            break
                        except socket.timeout:
                            err_info = 'Reloading for %d time'%count if count == 1 else 'Reloading for %d times'%count
                            print(err_info)
                            count += 1
                    if count > 5:
                        print('downloading failed!')
                except (UnicodeEncodeError, ConnectionResetError, http.client.HTTPException, urllib.error.URLError) as err:
                    print(err)
                else:
                    if not check_header(headers):
                        continue
                    print('downloaded')
                    print('#', self.parsed_num)
                    time = str(datetime.datetime.now())
                    print('time', time)
                    lines, urls, size = parse_page(local_filename)
                    print('urls', len(urls))
                    print('size', size)
                    prior_score = -prior_score
                    print('estimate', prior_score)
                    # calculate the relevance
                    cur_score = relevance(query, lines)
                    if cur_score >= 1:
                        self.rel_num += 1
                    print('relevance', cur_score)
                    self.log.append(
                        (
                            cur_url, time, size, '200', prior_score, cur_score,
                        )
                    )
                    # record the page
                    self.parsed_num += 1
                    if self.parsed_num >= MAX_NUM:
                        break
                    for url in urls:
                        if url in visited:
                            continue
                        elif url in d_scores:
                            # update the estimated promise
                            d_scores[url].append(cur_score)
                            continue
                        else:
                            d_scores[url] = [cur_score]

            level += 1
            print(len(d_scores))
            # calculate the promise
            for k in d_scores:
                v = d_scores[k]
                # use the average score
                url_relevance = url_score(query, k)
                if not url_relevance and len(v) < 10:
                    d_scores[k] = 0
                else:
                    d_scores[k] = sum(v) / len(v)  + 5 * len(v)
                    # 20 is a test constant
                    d_scores[k] = d_scores[k] + 10 * url_relevance
            # push to queue
            print('here')
            for url, score in d_scores.items():
                if not score:
                    continue
                n_url = normalize_url(url)
                if n_url.split('/')[0] not in site_num or site_num[n_url.split('/')[0]] < MAX_PER_SITE:
                    heapq.heappush(heap, (-score, url))
                    visited[n_url] = level
                    site_num[n_url.split('/')[0]] = site_num.get(n_url.split('/')[0], 0) + 1
        end = datetime.datetime.now()
        self.time = end - start


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Web Crawler')
    parser.add_argument('keyword', type=str, help='keyword to crawl')
    parser.add_argument('max_number', type=int, help='number of files to crawl')
    parser.add_argument('method', type=str, help='bfs or focus')
    args = parser.parse_args()
    url = 'https://www.gigablast.com/search'
    query = args.keyword
    MAX_NUM = args.max_number
    # limit the page number per site
    MAX_PER_SITE = 20
    # limit the socket time to 30s
    socket.setdefaulttimeout(30)
    params = {
        'format': 'json',
        'q': query,
        'c': 'main',
        'n': 10,
        'showerrors': 1,
        'userid':196,
        'code':1489906050,
    }
    res = requests.get(url, params = params)
    results = res.json()['results']
    pprint.pprint(results)
    crawler = Crawler()
    if args.method == 'bfs':
        crawler.bfs(query, MAX_NUM, MAX_PER_SITE)
        crawler.savelog('bfs.txt')
    elif args.method == 'focus':
        crawler.focus(query, MAX_NUM, MAX_PER_SITE)
        crawler.savelog('focus.txt')
