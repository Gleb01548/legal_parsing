import os
import time
import shutil
import requests
from multiprocessing import Process, Manager

import pendulum
import pandas as pd
from fake_useragent import UserAgent
from os.path import join as jp
from bs4 import BeautifulSoup
from tqdm import tqdm


class ParsDecisions:
    def __init__(
        self,
        path_file_proxy: str,
        path_save_file: str,
        attempt: int,
        time_sleep: int,
    ) -> None:
        self.path_file_proxy = path_file_proxy
        self.path_save_file = path_save_file
        self.attempt = attempt
        self.time_sleep = time_sleep

    def read_proxy_list(self, path_file_proxy: str) -> list:
        list_proxy = []
        for i in pd.read_csv(path_file_proxy, dtype="str")["proxy"].to_list():
            i = i.split(":")
            list_proxy.append(
                {"server": f"{i[0]}:{i[1]}", "username": i[-2], "password": i[-1]}
            )
        return list_proxy

    def creat_dir_if_not_exist(self, path: str, necessarily: bool = False) -> None:
        path = path.split("/")

        for i in range(2, len(path) + 1):
            path_now = jp(*path[:i])
            if not os.path.exists(path_now):
                os.mkdir(path_now)

        if necessarily:
            shutil.rmtree(path)
            os.mkdir(path)

    def list_files_walk(self, start_path, return_int=False):
        files = []

        for _, _, f in os.walk(start_path):
            for ff in f:
                if return_int:
                    ff = int(ff)
                files.append(ff)
        return files

    def create_number_for_link(
        self,
        path_save_file,
        pars_unsuccess_file: str = "no",
        start: int = 15792,
        end: int = 83700000,
    ) -> None:
        path_succes = jp(path_save_file, "_success")
        path_unsucces = jp(path_save_file, "_unsuccess")
        path_docs = jp(path_save_file, "docs")

        for i in [path_succes, path_unsucces, path_docs]:
            self.creat_dir_if_not_exist(i)

        if pars_unsuccess_file != "only":
            files_success = set(self.list_files_walk(path_succes, return_int=True))
            len_files_success = len(files_success)
            print(f"Ранее успешно спарщено: {len_files_success:,}")

            len_docs = len(self.list_files_walk(path_docs))

            print(
                f"Из них с решениями: {len_docs:,} ({round(len_docs / len_files_success * 100, 1)}%)"
            )
        else:
            files_success = self.list_files_walk(path_unsucces, return_int=True)
            print(f"Ранее НЕ успешно спарщено: {len(files_success):,}")
            return files_success

        if pars_unsuccess_file == "no":
            len_succes = len(files_success)
            files_success.update(
                set(self.list_files_walk(path_unsucces, return_int=True))
            )
            print(f"Ранее НЕ успешно спарщено: {len(files_success) - len_succes:,}")

        print("Подготовка номеров для формирования ссылок на судебные решения")
        list_pars = []
        for i in tqdm(range(start, end + 1)):
            if i not in files_success:
                list_pars.append(i)
        print("Done!")
        return list_pars

    def del_trash(self, s):
        return " ".join(s.split())

    def create_file(self, path: str, file_name: str) -> None:
        self.creat_dir_if_not_exist(path)

        pd.DataFrame().to_csv(jp(path, file_name))

    def check_maintenance(self) -> None:
        print("Проверка сайта на технические работы")
        list_number_check = [
            "84697716",
            "84697717",
            "84711991",
            "84711991",
            "84711993",
            "84712228",
        ]
        for number in list_number_check:
            res = requests.get(
                f"https://судебныерешения.рф/{number}/extended",
                headers={"User-Agent": UserAgent().random},
            )
            if not res.ok:
                continue
            else:
                return None
        warn = "НА САЙТЕ ТЕХНИЧЕСКИЕ РАБОТЫ"
        input(warn)

    def check_internet_every_n_sec(self, times: int, seconds: int) -> bool:
        print("Проверка интернет-соединения")
        for _ in range(times):
            time.sleep(seconds)
            try:
                if self.check_internet():
                    return True
            except requests.exceptions.ConnectionError:
                pass

    def get_page(
        self, url: str, proxy: int, attempt: int, time_sleep: int = 3
    ) -> BeautifulSoup:
        """
        Метод считывает заданную страницу,
        а потом преобразует в lxml
        """
        for i in range(attempt):
            try:
                res = requests.get(
                    url,
                    headers={"User-Agent": UserAgent().random},
                    proxies=proxy,
                )
                time.sleep(time_sleep)
            except requests.exceptions.ConnectionError:
                if self.check_internet_every_n_sec(120, 10):
                    continue

            if res is not None and res.ok:
                return BeautifulSoup(res.text, "lxml")
            elif i == attempt - 2:
                self.check_maintenance()

    def save_page(
        self, soup: BeautifulSoup, path_save_file: str, path_from_date: str, number: str
    ):
        path = jp(path_save_file, "pages", path_from_date)
        self.creat_dir_if_not_exist(path)

        pd.DataFrame({"page": [str(soup)]}, index=[0]).to_parquet(
            jp(path, number), index=False
        )

    def pars_head(self, soup: BeautifulSoup) -> dict:
        soup_head = soup.find("div", style="margin-top:20px")

        data_head = [
            ("Номер дела:", "number_deal"),
            ("УИН:", "uin"),
            ("Дата начала:", "date_start"),
            ("Дата рассмотрения:", "date_trial"),
            ("Суд:", "judgmsud"),
            ("Судья:", "judge"),
            ("Статьи УК:", "crime_article"),
            ("Статьи КоАП:", "adm_article"),
        ]

        data = {}

        for text in soup_head.find_all("p"):
            text = text.text
            for text_data, name in data_head:
                if text.startswith(text_data):
                    data[name] = text[len(text_data) :].strip()
                    break

        for text_data, name in data_head:
            if text_data == "Статьи УК:" or text_data == "Статьи КоАП:":
                find_crime = soup_head.find_all("div")[-1].text
                index_cime_info = find_crime.find(text_data)

                if index_cime_info > -1:
                    data[name] = find_crime[index_cime_info + len(text_data) :].strip()

        for key in data_head:
            if key[1] not in data:
                data[key[1]] = None

        return data

    def pars_table_info(self, soup: BeautifulSoup) -> dict:
        data_table = {"Категория": "cat", "Результат": "result"}

        data = {}
        key = False
        for i in soup.find("dl", class_="dl-horizontal"):
            if i.text.strip() == "Категория" or i.text.strip() == "Результат":
                key = data_table[i.text.strip()]
                continue
            if key and i.text.strip():
                data[key] = self.del_trash(i.text)
                key = False

        table_person_move = soup.find_all("table", class_="table table-condensed")

        person = []

        if table_person_move:
            for index, i in enumerate(table_person_move[0].find_all("td")):
                if not index % 4:
                    person.append([])
                person[-1].append(self.del_trash(i.text))

        movement = []

        if len(table_person_move) > 1:
            for index, i in enumerate(table_person_move[1].find_all("td")):
                if not index % 4:
                    movement.append([])
                movement[-1].append(self.del_trash(i.text))

        data["person"] = str(person)
        data["movement"] = str(movement)

        for key in data_table.keys():
            data[key] = None

        return data

    def load_text(
        self,
        soup: BeautifulSoup,
        path_save_file: str,
        path_from_date: str,
        proxy: str,
        number: str,
        attempt: int = 10,
        time_sleep: float = 2,
    ) -> dict:
        data = {"link_doc": "", "doc_name": ""}

        doc_path = jp(path_save_file, "docs", path_from_date)

        buttom_info = soup.find_all("div", class_="btn-group1")
        if len(buttom_info) < 2:
            print("NOT-text-decision")
            return data

        buttom_info = buttom_info[1].find("a")

        if not buttom_info:
            print("NOT-text-decision")
            return data

        soup_text = soup.select_one(
            "#content > div > div:nth-child(3) > div.col-md-9 > div > div:nth-child(7) > div"
        )
        if "print" in buttom_info["href"] and not soup_text:
            print("NOT-text-decision")
            return data

        if "print" in buttom_info["href"]:
            data["link_doc"] = ""
            doc_name = []

            for i in soup_text.find_all("p")[:3]:
                if "Дело" in i.text or "Акты" in i.text or "уид" in i.text:
                    pass
                else:
                    doc_name.append(self.del_trash(i.text))

            doc_name = "/n".join(doc_name)

            data["doc_name"] = doc_name.lower()

            self.creat_dir_if_not_exist(doc_path)

            doc_path = jp(doc_path, f"{number}.parquet")

            pd.DataFrame({"text": [str(soup_text)]}, index=[0]).to_parquet(
                doc_path, index=False
            )
            print("parquet-text-decision")

            data["path_doc"] = doc_path

        else:

            data["doc_name"] = self.del_trash(buttom_info.text)
            data["link_doc"] = buttom_info["href"]

            for _ in range(attempt):
                try:
                    res = requests.get(
                        "https://судебныерешения.рф" + "/" + data["link_doc"],
                        headers={"User-Agent": UserAgent().random},
                        proxies=proxy,
                    )
                    time.sleep(time_sleep)
                    break
                except:
                    continue

            self.creat_dir_if_not_exist(doc_path)

            doc_path = jp(doc_path, f"{number}.doc")
            open(doc_path, "wb").write(res.content)
            print("doc-text-decision")

            data["path_doc"] = doc_path

        if not data["path_doc"]:
            print("not-text-decision")

        return data

    def create_path_from_date(self, date: str) -> str:
        dt = pendulum.from_format(date, "DD.MM.YYYY")
        return f"{dt.year}/{dt.month}/{dt.day}"

    def pars(self, number, proxy):
        url = f"https://судебныерешения.рф/{number}/extended"
        print(url)
        print("")

        soup = self.get_page(url, proxy, self.attempt, self.time_sleep)

        data = {}
        data.update(self.pars_head(soup))
        path_from_date = self.create_path_from_date(data["date_start"])

        self.save_page(soup, self.path_save_file, path_from_date, number)

        data.update(self.pars_table_info(soup))
        data.update(
            self.load_text(
                soup,
                self.path_save_file,
                path_from_date,
                proxy=proxy,
                number=number,
                attempt=self.attempt,
                time_sleep=self.time_sleep,
            )
        )

        path_data = jp(self.path_save_file, "data", path_from_date)
        path_success = jp(self.path_save_file, "_success", path_from_date)

        self.creat_dir_if_not_exist(path_data)
        self.creat_dir_if_not_exist(path_success)

        data["link_page"] = url

        pd.DataFrame(data, index=[0]).to_parquet(
            jp(path_data, number) + ".parquet",
            index=False,
        )

        pd.DataFrame(data, index=[0]).to_parquet(
            jp(path_success, number),
            index=False,
        )

        path_unsuccess = jp(
            self.path_save_file, "_unsuccess", f"{int(number) % 1000}", number
        )
        if os.path.exists(path_unsuccess):
            os.remove(path_unsuccess)

    def pars_func(self, numbers, proxy, len_numbers, counter):
        while numbers:
            try:
                number = numbers.pop()
                # with counter.get_lock():
                counter.value += 1
            except:
                pass
            print(f"Обработка {counter.value:,} записи из {len_numbers:,}")
            # self.pars(number=str(number), proxy=proxy)

            try:
                self.pars(number=str(number), proxy=proxy)
            except Exception as e:
                print()

                problem_link = f"https://судебныерешения.рф/{number}/extended"
                print(
                    "Проблема:",
                    problem_link,
                    "\n",
                    e,
                )
                print()
                pd.DataFrame(
                    {"problem": [e], "problem_link": [problem_link]}, index=[0]
                ).to_csv("./log/log.csv", index=False, header=False, mode="a")
                self.create_file(
                    jp(self.path_save_file, "_unsuccess", f"{number % 1000}"),
                    file_name=str(number),
                )
                time.sleep(2)

    def parallel_pars(
        self,
        use_serv_api=True,
        revers_pars_list: bool = True,
        pars_unsuccess_file: str = "no",
    ):
        list_proxy = self.read_proxy_list(self.path_file_proxy)

        if use_serv_api:
            list_proxy += [None]

        numbers_for_pars = self.create_number_for_link(
            self.path_save_file, pars_unsuccess_file=pars_unsuccess_file
        )

        if not revers_pars_list:
            numbers_for_pars = numbers_for_pars[::-1]

        manager = Manager()
        numbers_pars = manager.list()
        counter = manager.Value("i", 0)
        numbers_pars.extend(numbers_for_pars)

        len_numbers = len(numbers_pars)
        print(f"Надо спарсить {len_numbers:,}")
        processes = [
            Process(
                target=self.pars_func,
                args=[numbers_pars, proxy, len_numbers, counter],
            )
            for proxy in list_proxy
        ]

        for process in processes:
            time.sleep(1)
            process.start()

        for process in processes:
            process.join()


if __name__ == "__main__":
    parser = ParsDecisions(
        path_file_proxy="./proxy.csv",
        path_save_file="./data/raw/decisions",
        attempt=10,
        time_sleep=4,
    )
    parser.parallel_pars(pars_unsuccess_file="yes")
