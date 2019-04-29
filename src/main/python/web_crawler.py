from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
import requests

seeds = ['/wiki/Animal']
visited = []
indexes = []
pairs = []


def extract_from_link(link):
    if link not in indexes:
        indexes.append(link)
    if 'https://en.wikipedia.org/' not in link:
        link = 'https://en.wikipedia.org' + link
    r = requests.get(link)
    data = r.text
    soup = BeautifulSoup(data, features="html5lib")
    for l in soup.find_all('a'):
        href = l.get('href')
        if href and href.startswith('/wiki/') and ':' not in href and 'Main_Page' not in href:
            if href not in indexes:
                indexes.append(href)
            if href not in seeds:
                seeds.append(href)
            link = link.replace('https://en.wikipedia.org', '')
            pairs.append([indexes.index(link), indexes.index(href)])
            # print(href)

    pass


while len(seeds) != 0 and len(visited) < 10:
    link = seeds[0]
    seeds.remove(link)
    if link not in visited:
        print(len(visited))
        extract_from_link(link)
        visited.append(link)

pd.DataFrame(np.array(pairs)).to_csv('file.csv',index=False)
