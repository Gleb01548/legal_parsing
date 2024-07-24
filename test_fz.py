import re
import time
from copy import deepcopy

import html2text
import playwright
import pandas as pd
import requests, time
from bs4 import BeautifulSoup
import bs4.element
from fake_useragent import UserAgent
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError


requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS = "ALL:@SECLEVEL=1"

h = html2text.HTML2Text()
h.ignore_links = True

def goto(page, url, attempt: int, time_if_fail: int) -> None:
    for n in range(attempt):
        try:
            page.goto(url)
            break
        except PlaywrightTimeoutError:
            print(f"Fail load info {n+1} from {attempt}")
            time.sleep(time_if_fail)
    
def pars(browser: object, url: str, use_button: object = None):
    context = browser.new_context(
        user_agent=UserAgent().random, viewport={"width": 1920, "height": 1080}
    )
    page = context.new_page()

    goto(page, url, attempt=5, time_if_fail=30)
    # Ожидание загрузки страницы и появления элемента
    time.sleep(2)

    if type(use_button) != type(None):
        page = use_button(page)
    
    iframe = page.query_selector('iframe').content_frame()

    soup = BeautifulSoup(iframe.content(), features="html.parser")
    context.close()
    return soup


with sync_playwright() as playwright:
    browser = playwright.firefox.launch(headless=True)
    soup = pars(browser=browser, url="http://actual.pravo.gov.ru/list.html#date_period=%2C24.07.2024&kinds=107&sort=-date")
    # print(soup.find("body"))
    
    doc_list = [i.text for i in soup.find("body").find_all("p")]
    
    res = {"end_doc": "\n".join(doc_list[-6:])}
    
    head = ""
    for index, i in enumerate(doc_list):
        if i.startswith("Статья "):
            break
        head += "/n"
        head += i
        
    res["head"] = head.replace("/n", "\n").strip()
    
    print(res["head"])
    
    res = []
    st_info = None
    for i in doc_list[index:-6]:
        if i.startswith("Статья "):
            st_info = {"num_st": float(i.split()[1]), "name_st": i}
            res.append(st_info)
        else:
            may_int = i.split()[0][:-1]
            st_without_parts = "text" not in res[-1]
            
            if may_int.replace(".", "").isdigit():
                pass
                
    print(res)
            
    
    # print(h.handle(soup.find("body").find_all("p")))