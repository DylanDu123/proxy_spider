from multiprocessing import Pool
import os, time, random
import requests
import config


def long_time_task():

    url = 'http://www.ip181.com/daili/10.html'
    random_headers = config.HEADER
    random_headers['User-Agent'] = random.choice(config.USER_AGENTS)
    r = requests.get(url,headers = random_headers,timeout = config.TIMEOUT)
    print(r.status_code)
    print(r.text)

if __name__=='__main__':
    long_time_task()