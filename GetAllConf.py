from typing import Optional, List, Dict, Callable, Union
from enum import IntEnum
import os
import re
import json
from datetime import datetime
import requests
from loguru import logger


COMMON_TEMPLATE = "https://dblp.uni-trier.de/search/publ/api?q=toc%3Adb/"
COMMON_URL_PREFIX = "://dblp.uni-trier.de/db/"
LIST_FILE = "./conf_jnl.txt"
SAVE_ROOT = "./RECORDS"


class PaperType(IntEnum):
    Conf = 0
    Jnl = 1


def get_full_json_path(conf_sym: str, year_or_vol: int,
                       paper_type: PaperType = PaperType.Conf):
    assert isinstance(year_or_vol, int), "Year should be an integer, \
                        but {} is {}".format(year_or_vol, type(year_or_vol))
    midd_str = ["conf/", "journals/"][paper_type != PaperType.Conf]
    if paper_type == PaperType.Conf:
        assert 1900 < year_or_vol <= 2023,\
            "Invalid year: {}".format(year_or_vol)
    bht_prefix = conf_sym + str(year_or_vol)
    suffix = "{}/{}.bht%3A&h=1000&format=json".format(conf_sym, bht_prefix)
    res = COMMON_TEMPLATE + midd_str + suffix
    return res


def parse_conf_journal_list(conf_jnl_list: str):
    assert os.path.exists(conf_jnl_list),\
            "Path does not exist: {}".format(conf_jnl_list)
    with open(conf_jnl_list, "r") as fp:
        conf_jnl_lines = fp.readlines()
    conf_jnl_record = list()
    for line in conf_jnl_lines:
        line = re.sub(r"[\n]+", "", line)
        line = re.sub(r"[ |\t]+", " ", line)
        if len(re.findall(r"^[A-C]:", line)) > 0:
            continue
        parts = line.split(" ")
        if len(parts) != 2:
            logger.warning("Not a conference ? {}".format(line))
            continue
        name, link = parts
        """
        A dblp link is https or http, using re.search to find the
        correct position that the conference name starts from.
        Try twice, first for conference, if failed, try to get
        a journal link. If both are failed, it indicates some
        configuration error occurs, the user should check their
        links to ensure they are valid dblp URLs.
        """
        position = re.search(COMMON_URL_PREFIX + "conf/", link)
        if position is None:
            position = re.search(COMMON_URL_PREFIX + "journals/", link)
            if position is None:
                logger.error("Unexpected link: {}. \
                             Neither a journal nor a conference.".format(link))
                raise RuntimeError("Link fault: {}.".format(link))
            paper_type = PaperType.Jnl
        else:
            paper_type = PaperType.Conf
        sym_raw = link[position.span()[1]:]
        sym = sym_raw.split("/")[0]
        conf_jnl_record.append([name, sym, paper_type])
    return conf_jnl_record


def is_json_valid(json_dict: dict) -> bool:
    """
    Determine whether a returned result is a valid result.
    """
    res: Optional[dict] = json_dict.get("result", False)
    if res is False:
        return False
    else:
        hits_res: Optional[dict] = res.get("hits", False)
        if hits_res is False:
            return False
        else:
            hits_cnt = hits_res.get("@total", False)
            if hits_cnt is False:
                return False
            else:
                hits_cnt_int = int(hits_cnt)
                if hits_cnt_int == 0:
                    return False
                return True


def prepare_jsons(name: str, year_or_vol: int, json_url: str, save_dir: str):
    logger.info("Fetch URL: {}".format(json_url))
    req_res = requests.get(json_url)
    parsed_json: dict = req_res.json()
    if not is_json_valid(parsed_json):
        logger.warning("Requested result empty: {} - {}".format(name,
                                                                year_or_vol))
        return
    legal_name = re.sub(r"[ ]+", "_", name)
    assert os.path.exists(save_dir), "Dir does not exist: {}".format(save_dir)
    conf_jnl_dir = os.path.join(save_dir, legal_name)
    if not os.path.exists(conf_jnl_dir):
        os.mkdir(conf_jnl_dir)
    conf_jnl_year_dir = os.path.join(conf_jnl_dir, str(year_or_vol))
    if not os.path.exists(conf_jnl_year_dir):
        os.mkdir(conf_jnl_year_dir)
    with open(os.path.join(conf_jnl_year_dir, "papers.json"), "w") as fp:
        json.dump(parsed_json, fp)
    return conf_jnl_year_dir, parsed_json


def get_all_conf_jnl_jsons(conf_jnl_list_file: str):
    min_year = 2013
    max_year = 2023
    try:
        import pandas as pd

        def export_excel(csv_file: str):
            pd_csv = pd.read_csv(csv_file, sep="\t")
            xls_file = csv_file[:-3] + "xlsx"
            pd_csv.to_excel(xls_file, index=None,
                            header=["Title", "Author", "DOI"])
    except ImportError:
        export_excel = None
        logger.info("Pandas not found, result is saved as CSV but not EXCEL.")
    else:
        logger.info("Pandas imported, both CSV and XLSX files will be saved.")
    year_range = range(min_year, max_year + 1)
    conf_jnl_list = parse_conf_journal_list(conf_jnl_list_file)
    info_str = ["Conference", "Journal"]
    for conf_jnl_info in conf_jnl_list:
        name, sym, paper_type = conf_jnl_info
        logger.info("Get {}: {}-{}".format(info_str[int(paper_type)],
                                           name, sym))
        if paper_type == PaperType.Conf:
            for year in year_range:
                json_url = get_full_json_path(sym, year)
                res = prepare_jsons(name, year, json_url, SAVE_ROOT)
                if res is not None:
                    post_process_json(*res, export_func=export_excel)
        else:
            vol_list: List[int] = get_journal_indexed_json(sym, min_year)
            if vol_list is False:
                continue
            for vol in vol_list:
                json_url = get_full_json_path(sym, vol, PaperType.Jnl)
                res = prepare_jsons(name, vol, json_url, SAVE_ROOT)
                if res is not None:
                    post_process_json(*res, export_func=export_excel)


def get_journal_indexed_json(journal_sym: str, req_year_min: int) -> List[int]:
    json_url = "https://dblp.uni-trier.de/search/publ/api?q=stream%3Astr"\
            "eams%2Fjournals%2F{}%3A&h=1000&format=json".format(journal_sym)
    req_res = requests.get(json_url)
    json_res: dict = req_res.json()
    if is_json_valid(json_res):
        logger.info("Journal {} is found.".format(journal_sym))
    else:
        logger.warning("Journal {} is not found.".format(journal_sym))
        return False
    all_papers: List[dict] = json_res.get("result").get("hits").get("hit")
    effective_vols = list()
    for paper in all_papers:
        paper_info: dict = paper.get("info", False)
        if paper_info is False:
            logger.warning("Invalid paper info in {}.".format(journal_sym))
            continue
        year = get_year(paper_info)
        if year == "":
            logger.warning("Invalid year info: {}.".format(journal_sym))
            continue
        vol = get_volume(paper_info)
        if vol != "":
            if vol not in effective_vols:
                effective_vols.append(vol)
        else:
            logger.warning("Invalid volume info: {}.".format(journal_sym))
            continue
        if year <= req_year_min:
            break
    if len(effective_vols) == 0:
        logger.warning("No valid volumes found in: {}.".format(journal_sym))
    return effective_vols


def get_authors(full_info: dict):
    authors: dict = full_info.get("authors", False)
    if authors is False:
        logger.warning("Paper has no authors.")
        return ""
    author_list: List[Dict] = authors.get("author", False)
    if author_list is False:
        return ""
    else:
        res = ""
        if not isinstance(author_list, list):
            if isinstance(author_list, dict):
                if author_list.get("text", False) is not False:
                    maybe_author = author_list["text"]
                    if isinstance(maybe_author, str):
                        return re.sub(r"[\t]+", " ", author_list["text"])
                    else:
                        return ""
                else:
                    return ""
            else:
                return ""
        for single_author in author_list:
            name_text = single_author.get("text", False)
            if name_text is False:
                continue
            else:
                if name_text == "":
                    continue
                else:
                    res += re.sub(r"[\t]+", " ", name_text) + ","
        if res != "":
            return res[:-1]
        else:
            return res


def get_value_from_key(full_info: dict, key: str,
                       return_type: Callable) -> Union[int, str]:
    assert return_type in [int, str], \
        "Unsupported type cast: {}".format(return_type)
    requested_val: dict = full_info.get(key, False)
    if requested_val is False:
        logger.warning("Paper has no {}.".format(key.upper()))
        return ""
    try:
        res = return_type(requested_val)
    except TypeError:
        logger.error("Cannot apply type cast: {}".format(requested_val))
        res = ""
    return res


def get_doi(full_info: dict) -> str:
    return get_value_from_key(full_info, "doi", str)


def get_year(full_info: dict) -> Union[int, str]:
    return get_value_from_key(full_info, "year", int)


def get_volume(full_info: dict) -> Union[int, str]:
    return get_value_from_key(full_info, "volume", int)


def post_process_json(saved_dir: str, json_dict: dict, export_func=None):
    hits: dict = json_dict.get("result").get("hits")
    total_hits = int(hits.get("@total"))
    if total_hits == 0:
        logger.warning("Probably something wrong\
                       as total number is 0: {}".format(total_hits))
        return
    records: List[dict] = hits.get("hit", False)
    if records is False:
        logger.warning("No records found.")
        return
    paper_line = ""
    for record in records:
        paper_info: dict = record.get("info")
        title = paper_info.get("title", False)
        if title is False:
            logger.warning("No title found, skip.")
            continue
        title = re.sub(r"[\t]+", " ", title)
        paper_line += title + "\t"
        authors = get_authors(paper_info)
        paper_line += authors + "\t"
        paper_line += get_doi(paper_info) + "\n"
    csv_file = os.path.join(saved_dir, "papers.csv")
    with open(csv_file, "w") as fp:
        fp.write(paper_line)
    if export_func is not None:
        export_func(csv_file)


def init_log():
    now_time = datetime.now().strftime("%m-%d-%Y_%H-%M-%S")
    log_file = os.path.join(SAVE_ROOT, now_time + ".log")
    logger.add(log_file, format="[{time}] [{level}]: {message}")


if __name__ == "__main__":
    init_log()
    get_all_conf_jnl_jsons(LIST_FILE)
