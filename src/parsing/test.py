import os
import re
import time
import yaml
from copy import deepcopy
from os.path import join as jp

import pandas as pd
import playwright
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

from fake_useragent import UserAgent
from bs4 import BeautifulSoup
from tqdm import tqdm


class ParsCodex:
    def __init__(self):
        pass
    
    def goto(self, page, url, attempt: int, time_if_fail: int) -> None:
        for n in range(attempt):
            try:
                page.goto(url)
                break
            except PlaywrightTimeoutError:
                print(f"Fail load info {n+1} from {attempt}")
                time.sleep(time_if_fail)

    def pars(self, browser: object, url: str, use_button: object = None):
        context = browser.new_context(
            user_agent=UserAgent().random, viewport={"width": 1920, "height": 1080}
        )
        page = context.new_page()

        self.goto(page, url, attempt=5, time_if_fail=30)
        # Ожидание загрузки страницы и появления элемента
        time.sleep(2)

        if type(use_button) != type(None):
            page = use_button(page)

        soup = BeautifulSoup(page.content(), features="html.parser")
        context.close()
        return soup

    def check_stop_button(self, page) -> object:
        if not BeautifulSoup(page.content(), features="html.parser").find_all(
            "div", class_="module relsuddoc"
        ):
            return page
        old_len = 0
        bs_page = BeautifulSoup(page.content(), features="html.parser")
        new_len = len(bs_page.find("div", class_="module relsuddoc").find_all("a"))
        while old_len != new_len and bs_page.find_all(
            "button", id="add-relsuddoc-list2"
        ):
            old_len = new_len

            page.evaluate(
                """() => {
                    const button = document.getElementById('add-relsuddoc-list2');
                    if (button) {
                        button.classList.remove('hide');
                    }
                }"""
            )
            page.click("#add-relsuddoc-list2")
            time.sleep(2)
            new_len = len(
                BeautifulSoup(page.content(), features="html.parser")
                .find("div", class_="module relsuddoc")
                .find_all("a")
            )
        return page  

    def pars_parts_st(self, soup, st_info: dict) -> list:
        data = []
        for index, i in enumerate(
            soup.find("div", class_="st-body content-body").find_all("p")
        ):
            i = i.text
            may_int = i.split()[0][:-1]
            if may_int.replace(".", "").isdigit():
                part_st_info = {"part_st": float(may_int), "text": i}
            elif not index:
                part_st_info = {"part_st": 0, "text": i}
            else:
                data[-1]["text"] = data[-1]["text"] + "\n" + i
                continue
            part_st_info.update(deepcopy(st_info))
            data.append(part_st_info)
        return data

    def pars_comment(self, soup, st_info) -> str:
        soup_comment = soup.find("div", class_="rellawcomment-content content-body")
        cont = False
        comment_list = []
        
        if not soup_comment:
            return None
        
        for i in soup_comment.find_all("p"):
            i = i.text
            if cont:
                cont = False
                continue
            if "--------" in i:
                cont = True
                continue
            comment_list.append(re.sub("<.*?>", "", i).replace("  ", "").strip())

        comment = "".join(comment_list)
        st_info_comm = deepcopy(st_info)
        st_info_comm["text_comment"] = comment
        return st_info_comm

    def parse_link_decisions(self, soup, st_info, base_url: str) -> list:
        if not soup.find("div", class_="module relsuddoc"):
            return []

        decisions_links = []
        for i in soup.find("div", class_="module relsuddoc").find_all("a"):
            link_decision_info = deepcopy(st_info)
            link_decision_info.update(
                {"href": base_url + i.attrs["href"], "name": i.text.strip()}
            )
            decisions_links.append(link_decision_info)
        return decisions_links

    def parse_desicions(self, brows, desicions_list: list) -> None:
        for i in tqdm(desicions_list):
            soup = self.pars(brows, url=i["href"])
            i["text_decisions"] = soup.find(
                "article", class_="suddoc_content content-body"
            ).text

    def run(self, st_info_list, base_url, proxy, path_dir_save):
        
        while st_info_list:
            st_info = st_info_list.pop()
    
            # получаем страницу с конкретной статьей
            soup_st = self.pars(
                browser=browser, url=st_info["href"], use_button=self.check_stop_button
            )

            # парсим статьи (каждую часть отдельно)
            res_st = self.pars_parts_st(soup=soup_st, st_info=st_info)

            # парсим комментарий к статье
            res_comment = self.pars_comment(soup=soup_st, st_info=st_info)

            # парсим ссылки на судебные решения, которые указаны на странице со статьей
            desicions_list = self.parse_link_decisions(
                soup=soup_st, st_info=st_info, base_url=base_url
            )

            # парсим судебные решения
            self.parse_desicions(brows=browser, desicions_list=desicions_list)

            if st_info["num_st"].is_integer():
                name = str(int(st_info["num_st"])).replace(".", "_")
            else:
                name = str(st_info["num_st"]).replace(".", "_")

            name = name + ".csv"

            if desicions_list:
                pd.DataFrame(desicions_list).to_csv(
                    jp(path_dir_save, "decisions", name), index=False
                )
            if res_comment:
                pd.DataFrame(res_comment, index=[0]).to_csv(
                    jp(path_dir_save, "comment", name), index=False
                )
            pd.DataFrame(res_st).to_csv(jp(path_dir_save, "st", name), index=False)
            pd.DataFrame().to_csv(jp(path_dir_save, "_success", name.removesuffix(".csv")))

class ParsCodexParallel:
    def __init__(self) -> None:
        pass
    
    def get_codex_name_info(self, soup) -> dict:
        codex_name_info = {}
        for index, i in enumerate(
            soup.find("div", class_="law-desc content-body").find_all(
                "p", class_="align-center"
            )
        ):
            if index == 0:
                codex_name_info["name_codex"] = i.text

            if index == 1:
                for name, num in zip(
                    ["ПЕРВАЯ", "ВТОРАЯ", "ТРЕТЬЯ", "ЧЕТВЕРТАЯ"], [1, 2, 3, 4]
                ):
                    if name in i.text:
                        codex_name_info["part"] = num
                        break

        return codex_name_info

    def check_attr(self, list_attr: str) -> bool:
        for i in ["r", "pr", "gl", "st", "pf"]:
            for k in list_attr:
                if k == i:
                    return True
        return False

    def pars_link_st(self, url_codex: str, soup, codex_name_info: dict) -> list:
        rome_to_arab = {
            "I": 1,
            "II": 2,
            "III": 3,
            "IV": 4,
            "V": 5,
            "VI": 6,
            "VII": 7,
            "VIIII": 8,
            "IX": 9,
            "X": 10,
            "XI": 11,
            "XII": 12,
            "XIII": 13,
        }
        table_content = []

        num_r, name_r = None, None
        num_pr, name_pr = None, None
        num_gl, name_gl = None, None
        num_pf, name_pf = None, None
        num_st, name_st = None, None
        href = None

        for i in [i for i in soup.find_all("li") if self.check_attr(i.attrs["class"])]:
            element = deepcopy(codex_name_info)

            if "r" in i.attrs["class"]:
                num_r = rome_to_arab.get(i.text.split(".")[0].split()[1], None)
                name_r = i.text.strip()
                continue

            if "pr" in i.attrs["class"]:
                try:
                    num_pr = int(i.text.replace(".", "").split()[1])
                except:
                    pass

                name_pr = i.text.strip()
                continue

            if "gl" in i.attrs["class"]:
                try:
                    num_gl = int(i.text.replace(".", "").split()[1])
                except:
                    pass

                name_gl = i.text.strip()
                continue

            if "pf" in i.attrs["class"]:
                try:
                    num_pf = int(i.text.replace(".", "").split()[1])
                except:
                    pass

                name_pf = i.text.strip()
                continue

            if "st" in i.attrs["class"]:
                try:
                    num_st = float(i.text.split()[1].removesuffix("."))
                except:
                    pass

                name_st = i.text.strip()
                href = url_codex + i.find("a").attrs["href"][1:]

                for name in [
                    "num_r",
                    "name_r",
                    "num_pr",
                    "name_pr",
                    "num_gl",
                    "name_gl",
                    "num_pf",
                    "name_pf",
                    "num_st",
                    "name_st",
                    "href",
                ]:
                    element[name] = eval(name)

                table_content.append(element)
        return table_content

    def check_st(self, table_content: dict, path_dir_save: str) -> dict:
        print("Всего требуется спарсить статей:", len(table_content))
        list_st_files = os.listdir(jp(path_dir_save, "_success"))
        new_table_content = []
        utr = 0
        for st_info in table_content:
            if "утратила силу" in st_info["name_st"].lower():
                utr += 1
                continue
            
            if st_info["num_st"].is_integer():
                name = str(int(st_info["num_st"])).replace(".", "_")
            else:
                name = str(st_info["num_st"]).replace(".", "_")
                
            if not name in list_st_files:
                new_table_content.append(st_info)
        print("Из них:")
        print("Таки утратили силу и потому не нужны:", utr)
        print("Таки спарщено уже:", len(table_content) - len(new_table_content))
        print("Таки нет:", len(new_table_content))
        return new_table_content

    def run(self, browser, url, base_url, path_dir_save, list_proxy):
        with sync_playwright() as playwright:

            browser = playwright.firefox.launch(headless=True)
            soup_title_codex = self.pars(browser=browser, url=url)
            codex_name_info = self.get_codex_name_info(soup=soup_title_codex)

            table_content = self.pars_link_st(
                soup=soup_title_codex, url_codex=url, codex_name_info=codex_name_info
            )

            table_content = self.check_st(table_content, path_dir_save)
            browser.close()
            
        for proxy in list_proxy:
            pass

        for st_info in tqdm(table_content):
            pass





with open('conf.yaml') as fh:
    read_data = yaml.load(fh, Loader=yaml.FullLoader)

with sync_playwright() as playwright:

    browser = playwright.firefox.launch(headless=True)
    pars_npa = ParsCodexParallel(file_path=None)
    pars_npa.run(
        browser=browser,
        url=read_data["url"],
        base_url=read_data["base_url"],
        path_dir_save=read_data["path_dir_save"],
    )
    # for _ in range(100):
    #     try:
    #         browser = playwright.firefox.launch(headless=True)
    #         pars_npa = ParsCodex(file_path=None)
    #         pars_npa.run(
    #             browser=browser,
    #             url=read_data["url"],
    #             base_url=read_data["base_url"],
    #             path_dir_save=read_data["path_dir_save"],
    #         )
    #     except:
    #         print("АХТУНГ!!!")
