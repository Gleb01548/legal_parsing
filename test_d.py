import time

from playwright.sync_api import sync_playwright
from fake_useragent import UserAgent


with sync_playwright() as p:
    browser = p.firefox.launch(headless=True)
    context = browser.new_context(user_agent=UserAgent().random, viewport={"width": 1920, "height": 1080})
    page = context.new_page()
    page.goto("https://судебныерешения.рф/77396477/extended", wait_until="load")
    time.sleep(1)
    print(page.title())
    browser.close()