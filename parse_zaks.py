#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import traceback
from functools import lru_cache

from pandas import DataFrame
from bs4 import BeautifulSoup
from requests import get


@lru_cache()
def get_html(url):
    for retry in range(1, 10):
        print('try %d get %s' % (retry, url))
        response = get(url)
        if response.status_code == 200 and response.text:
            soup = BeautifulSoup(response.text)
            if soup.find('tbody', id='test'):
                return soup
    print('Max reties exceeded')
    sys.exit(1)


def get_cells(tr):
    return [x.text.strip() for x in tr.find_all('td')]


def parse_candidates(first_page):
    soup = get_html(first_page)
    page2_link = soup.find('a', text='2')
    if page2_link:
        page_urls = [first_page] + [x['href'] for x in page2_link.parent.find_all('a')]
    else:
        page_urls = [first_page]
    print('%d pages' % len(page_urls))

    data = []

    for page_url in page_urls:
        soup = get_html(page_url)
        try:
            new_data = [get_cells(x)[:7] for x in soup.find('tbody', id='test').find_all('tr')]
        except:
            traceback.print_exc()

        if new_data:
            print('... %s' % '\t'.join(new_data[-1]))
            data += new_data

    return DataFrame(data, columns=['ord', 'fio', 'bday', 'party', 'okrug', 'vidvinut', 'registr'])


zaks_spb = 'http://www.vybory.izbirkom.ru/region/region/st-petersburg?action=show&root=1&tvd=2782000678450&vrn=2782000678445&region=78&global=true&sub_region=78&prver=0&pronetvd=0&vibid=2782000678445&type=220'


dataframe = parse_candidates(zaks_spb)

with open('candidates-zaks-spb.csv', 'w+') as filee:
    filee.write(dataframe.set_index('ord').to_csv())
