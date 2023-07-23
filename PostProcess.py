import os
import shutil


ROOT_DIR = "./RECORDS"


def move_files(root_dir: str):
    all_subs = os.listdir(root_dir)
    for sub_dir in all_subs:
        if sub_dir[-3:] == "log":
            continue
        sub_subs = os.listdir(os.path.join(root_dir, sub_dir))
        for sub_sub_dir in sub_subs:
            if sub_sub_dir[-4:] == "xlsx":
                os.remove(os.path.join(root_dir, sub_dir, sub_sub_dir))
                continue
            full_path = os.path.join(root_dir, sub_dir, sub_sub_dir,
                                     "papers.xlsx")
            if not os.path.exists(full_path):
                print("Path does not exist: {}".format(full_path))
                continue
            new_path = os.path.join(root_dir, sub_dir,
                                    "{}-{}.xlsx".format(sub_dir, sub_sub_dir))
            shutil.copy(full_path, new_path)


if __name__ == "__main__":
    move_files(ROOT_DIR)
