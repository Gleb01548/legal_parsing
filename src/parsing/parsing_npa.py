import os
import re
import time
import shutil
import yaml
from os.path import join as jp
from os.path import exists as ep
from copy import deepcopy

import html2text
import playwright
import pandas as pd
import requests, time
from bs4 import BeautifulSoup
import bs4.element
from fake_useragent import UserAgent
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError


class ParsNpa:
    def __init__(self) -> None:
        pass

    def creat_dir_if_not_exist(self, path: str, necessarily: bool = False) -> None:
        if not os.path.exists(path):
            os.mkdir(path)
        elif necessarily:
            shutil.rmtree(path)
            os.mkdir(path)

    def create_table(self, path, path_table_npa, columns, create_file_if_exist=False) -> None:

        self.creat_dir_if_not_exist(path)

        if create_file_if_exist or not ep(path_table_npa):
            pd.DataFrame(columns=columns).to_csv(path_table_npa, index=False)

    def goto(self, page, url, attempt: int, time_if_fail: int) -> None:
        for n in range(attempt):
            try:
                page.goto(url)
                break
            except PlaywrightTimeoutError:
                print(f"Fail load info {n+1} from {attempt}")
                time.sleep(time_if_fail)

    def pars_npa(
        self, page, path_save_doc, path_save_info, columns, extra_info_npa
    ) -> None:
        buttons = page.query_selector_all(
            "#appRoot > div > div > div > div > div.main-col.list-col > div > div.doc-preview-scroll.scroll-block.y-scroll > div.doc-preview-list > div"
        )

        for button in buttons:
            button.click()
            info_npa = self.extract_data_from_button(
                button, extra_info_npa=extra_info_npa
            )
            time.sleep(10)
            try:
                with page.expect_download() as download_info:
                    page.get_by_title("Сохранить текст").click(timeout=30000 * 100)
            except Exception as e:
                print(e)
                continue
            download = download_info.value

            time.sleep(5)

            npa_file_name = download.suggested_filename
            download.save_as(jp(path_save_doc, npa_file_name))

            info_npa["npa_file_name"] = npa_file_name

            pd.DataFrame(info_npa, index=[0])[columns].to_csv(
                path_save_info, index=False, mode="a", header=False
            )

    def extract_data_from_button(self, button, extra_info_npa) -> dict:
        list_info = button.inner_text().split("\n")
        res = {
            "status": list_info[0],
            "date_number": list_info[1],
            "name_npa": list_info[2],
        }
        res.update(extra_info_npa)
        return res

    def run(
        self,
        browser: object,
        url: str,
        columns: list,
        path: str,
        name_table_npa: str,
        extra_info_npa: dict,
    ):

        path_table_npa = jp(path, name_table_npa)

        self.create_table(
            path=path, path_table_npa=path_table_npa, columns=columns, create_file_if_exist=True
        )

        context = browser.new_context(
            user_agent=UserAgent().random, viewport={"width": 1920, "height": 1080}
        )
        page = context.new_page()

        self.goto(page, url, attempt=5, time_if_fail=30)
        # Ожидание загрузки страницы и появления элемента
        time.sleep(10)

        n_pages = 1
        while True:
            print(f"Номер страницы {n_pages}")
            page.wait_for_selector(
                "#appRoot > div > div > div > div > div.main-col.list-col > div > div.doc-preview-scroll.scroll-block.y-scroll > div.doc-preview-list > div"
            )
            time.sleep(2)

            self.pars_npa(
                page=page,
                path_save_doc=path,
                path_save_info=path_table_npa,
                extra_info_npa=extra_info_npa,
                columns=columns
            )

            button = page.query_selector(
                "#appRoot > div > div > div > div > div.main-col.list-col > div > div.control-list-contaner > div.control-list-block.row.pager-row > div:nth-child(3) > div > ul > li:nth-child(8) > a"
            )

            if "disabled" in button.get_attribute("class"):
                break

            button.click()
            n_pages += 1


def goto(page, url, attempt: int, time_if_fail: int) -> None:
    for n in range(attempt):
        try:
            page.goto(url)
            break
        except PlaywrightTimeoutError:
            print(f"Fail load info {n+1} from {attempt}")
            time.sleep(time_if_fail)


def pars_npa(page) -> None:
    buttons = page.query_selector_all(
        "#appRoot > div > div > div > div > div.main-col.list-col > div > div.doc-preview-scroll.scroll-block.y-scroll > div.doc-preview-list > div"
    )

    # Перебор найденных элементов
    for button in buttons:
        # Например, нажимаем на каждую кнопку
        button.click()  # Или выполняйте другие действия с элементами

        time.sleep(10)
        try:
            # Start waiting for the download
            with page.expect_download() as download_info:
                # Perform the action that initiates download
                page.get_by_title("Сохранить текст").click(timeout=300000000)
        except Exception as e:
            print(e)
            continue
        download = download_info.value

        time.sleep(5)

        # Wait for the download process to complete and save the downloaded file somewhere
        download.save_as("./data/fz/" + download.suggested_filename)


def pars(browser: object, url: str):
    context = browser.new_context(
        user_agent=UserAgent().random, viewport={"width": 1920, "height": 1080}
    )
    page = context.new_page()

    goto(page, url, attempt=5, time_if_fail=30)
    # Ожидание загрузки страницы и появления элемента
    time.sleep(10)

    n_pages = 1
    while True:
        print(f"Номер страницы {n_pages}")
        page.wait_for_selector(
            "#appRoot > div > div > div > div > div.main-col.list-col > div > div.doc-preview-scroll.scroll-block.y-scroll > div.doc-preview-list > div"
        )
        time.sleep(2)

        pars_npa(page=page)

        button = page.query_selector(
            "#appRoot > div > div > div > div > div.main-col.list-col > div > div.control-list-contaner > div.control-list-block.row.pager-row > div:nth-child(3) > div > ul > li:nth-child(8) > a"
        )

        if "disabled" in button.get_attribute("class"):
            break

        button.click()


with open("conf.yaml") as fh:
    read_data = yaml.load(fh, Loader=yaml.FullLoader)

need = read_data["pars_npa"]["fz"]
extra_info_npa = {i: need[i] for i in ["npa_type", "resource"]}

with sync_playwright() as playwright:
    browser = playwright.firefox.launch(headless=True)

    # context = browser.new_context(
    #     user_agent=UserAgent().random, viewport={"width": 1920, "height": 1080}
    # )

    pars = ParsNpa()

    pars.run(
        browser=browser,
        url=need["url"],
        path=need["path_save_docs"],
        name_table_npa=need["name_table_npa"],
        columns=[
            "npa_type",
            "name_npa",
            "date_number",
            "status",
            "npa_file_name",
            "resource",
        ],
        extra_info_npa=extra_info_npa,
    )

    # page = context.new_page()

    # url="http://actual.pravo.gov.ru/list.html#date_period=%2C24.07.2024&kinds=107&sort=type&hash=217abce7bb52d85a35383e5ac630c3a3cd4dde6c703e6a815eea921164bd94c4"

    # goto(page, url, attempt=5, time_if_fail=30)

    #     # Start waiting for the download
    # with page.expect_download() as download_info:
    #     # Perform the action that initiates download
    #     page.get_by_title("Сохранить текст").click()
    # download = download_info.value

    # time.sleep(5)

    # # Wait for the download process to complete and save the downloaded file somewhere
    # download.save_as("./" + download.suggested_filename)
