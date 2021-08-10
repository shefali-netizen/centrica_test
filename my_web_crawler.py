# -*- coding: utf-8 -*-
"""
Created on Fri Aug  6 19:53:59 2021

@author: shefa
"""
#!pip install google 
import googlesearch
import requests
from bs4 import BeautifulSoup

def web_crawler():
    try:
        from googlesearch import search
    except ImportError: 
        print("No module named 'google' found")
      
    # to search
    query = "A computer science portal"
      
    for URL in search(query, tld="co.in", num=10, stop=10, pause=2):
        
        r = requests.get(URL)
      
        soup = BeautifulSoup(r.content, 'html5lib') # If this line causes an error, run 'pip install html5lib' or install html5lib
        print(soup.prettify())
        
        
        
if __name__=='__main__':
    web_crawler()