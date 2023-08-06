import subprocess
import os
from subprocess import PIPE
from loguru import logger
from datetime import datetime


os.environ["PATH"] = os.environ["PATH"] + ":/home/zhaomingxin/.local/bin/"


DOWNLOAD_OPTION = " -u https://sci-hub.se -ow N"
FAULT_STR = "your searching has no result, please check! "
FAILED_STR = "failed requests for url:"
ROOT_DIR = "/home/zhaomingxin/CodeBase/ConfCollection/RECORDS/"
LOG_ROOT = "./"


PRIORITY_LIST = ["HPCA", "ASPLOS", "CGO", "PLDI", "OSDI", "USENIX_ATC",
                 "ISCA", "EuroSys", "PACT", "VEE", "SoCC", "CODES_ISSS",
                 "ICCD", "HOT_CHIPS"]


def prepare_doi_list(list_dir: str):
    if not os.path.exists(os.path.join(list_dir, "papers.csv")):
        logger.warning("papers.list does not exist in {}.".format(list_dir))
        return False
    with open(os.path.join(list_dir, "papers.csv"), "r") as fp:
        paper_record = fp.readlines()
    no_doi, doi_list = list(), list()
    doi_str = ""
    for record in paper_record:
        name, _, doi = record.replace("\n", "").split("\t")
        if doi == '':
            no_doi.append(name)
        else:
            doi_str += doi + "\n"
            doi_list.append([name, doi])
    if doi_str != "":
        doi_str = doi_str[:-1]
    else:
        return False
    with open(os.path.join(list_dir, "doi.txt"), "w") as fp:
        fp.write(doi_str)
    return no_doi, doi_list


def init_log():
    now_time = datetime.now().strftime("%m-%d-%Y_%H-%M-%S")
    log_file = os.path.join(LOG_ROOT, now_time + "download.log")
    logger.add(log_file, format="[{time}] [{level}]: {message}")


def download(root_dir: str, priority_list=None):
    command = "scihub -s "

    if priority_list is not None:
        conf_jnl_list = priority_list
    else:
        conf_jnl_list = os.listdir(root_dir)
    for conf_jnl in conf_jnl_list:
        full_path = os.path.join(root_dir, conf_jnl)
        if os.path.isfile(full_path):
            continue
        for year_vol in os.listdir(full_path):
            year_vol_dir = os.path.join(full_path, year_vol)
            if not os.path.isdir(year_vol_dir):
                continue
            logger.info("Start downloading: {}-{}".format(conf_jnl, year_vol))
            progress_file = os.path.join(year_vol_dir, "progress.log")
            if os.path.exists(progress_file):
                with open(progress_file, "r") as fp:
                    cur_progress = fp.read()
                if cur_progress.isdigit():
                    skip_idx = int(cur_progress)
                else:
                    with open(progress_file, "w") as fp:
                        fp.write("0")
                    skip_idx = 0
            else:
                skip_idx = 0
                with open(progress_file, "w") as fp:
                    fp.write("0")
            if os.path.exists(os.path.join(year_vol_dir, "status.log")):
                logger.info("{}-{} has been processed, skip.".format(conf_jnl,
                                                                     year_vol))
                continue
            res = prepare_doi_list(year_vol_dir)
            if res is False:
                logger.warning("No available paper "
                               "found in {}-{}.".format(conf_jnl,
                                                        year_vol_dir))
                with open(os.path.join(year_vol_dir, "status.log"), "w") as fp:
                    fp.write("NoDOI")
                continue
            pdf_path = os.path.join(year_vol_dir, "PDF")
            if not os.path.exists(pdf_path):
                os.mkdir(pdf_path)
            failed = list()
            for doi_idx, doi in enumerate(res[1]):
                logger.info("Downloading: {}".format(doi[0]))
                if doi_idx < skip_idx:
                    logger.info("File {} has been processed, "
                                "skip it.".format(doi[0]))
                    continue
                dres = subprocess.run([command + doi[1] +
                                       " -O {}".format(pdf_path) +
                                       DOWNLOAD_OPTION], shell=True,
                                      stderr=PIPE, stdout=PIPE,
                                      env=os.environ,
                                      check=False, text=True)
                with open(progress_file, "w") as fp:
                    fp.write(str(doi_idx + 1))
                if FAULT_STR in dres.stderr or FAILED_STR in dres.stderr:
                    failed.append(doi[0])
                    logger.warning("Download failed.")
                    if FAILED_STR in dres.stderr:
                        logger.warning("Request for URL failed.")
                    else:
                        logger.warning("No searching result.")
                    continue
                else:
                    logger.info("Download success.")
            with open(os.path.join(year_vol_dir, "status.log"), "w") as fp:
                fp.write("Processed")
            if len(res[0]) > 0:
                all_failed = res[0] + failed
            else:
                all_failed = failed
            if len(all_failed) == 0:
                continue
            if len(all_failed) == (len(res[0]) + len(res[1])):
                with open(os.path.join(year_vol_dir, "status.log"), "w") as fp:
                    fp.write("None")
            nofile = os.path.join(pdf_path, "nofile.txt")
            with open(nofile, "w") as fp:
                for idx, failed_name in enumerate(all_failed):
                    fp.write(failed_name + ("\n" if idx != len(all_failed) - 1
                             else ''))


if __name__ == "__main__":
    init_log()
    download(ROOT_DIR, PRIORITY_LIST)
