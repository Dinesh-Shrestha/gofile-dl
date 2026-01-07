import argparse
import fnmatch
import hashlib
import logging
import math
import os
import shlex
import signal
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

import requests
from pathvalidate import sanitize_filename
import shutil
from tqdm import tqdm


logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s][%(funcName)20s()][%(levelname)-8s]: %(message)s",
    handlers=[
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("GoFile")


class File:
    def __init__(self, link: str, dest: str):
        self.link = link
        self.dest = dest

    def __str__(self):
        return f"{self.dest} ({self.link})"


def aria2_download(
    url, output, quiet, proxy, token, resume, aria2_args
):
    cmd = [
        "aria2c",
        url,
        "-o",
        os.path.basename(output),
        "-d",
        os.path.dirname(output) or ".",
        f"--header=Cookie: accountToken={token}",
        "--file-allocation=none",
        "--auto-file-renaming=false",
        "--allow-overwrite=true",
    ]

    if quiet:
        cmd.append("--quiet=true")

    if proxy:
        cmd.append(f"--all-proxy={proxy}")

    if resume:
        cmd.append("--continue=true")

    if aria2_args:
        cmd.extend(shlex.split(aria2_args))
    else:
        # Balanced defaults
        cmd.extend(["-x", "5", "-s", "5", "--min-split-size=1M"])

    logger.info(f"Running aria2c command: {' '.join(cmd)}")

    try:
        proc = subprocess.Popen(cmd)
        proc.wait()
        return proc.returncode == 0
    except KeyboardInterrupt:
        if proc:
            proc.terminate()
            proc.kill()
        raise
    except Exception as e:
        logger.error(f"aria2c error: {e}")
        return False


class Downloader:
    def __init__(self, token):
        self.token = token
        self.progress_lock = Lock()
        self.progress_bar = None

    # send HEAD request to get the file size, and check if the site supports range
    def _get_total_size(self, link):
        r = requests.head(link, headers={"Cookie": f"accountToken={self.token}"}, allow_redirects=True)
        r.raise_for_status()
        return int(r.headers.get("Content-Length", 0)), r.headers.get("Accept-Ranges", "none") == "bytes", r.url

    # download the range of the file
    def _download_range(self, link, start, end, temp_file, i):
        existing_size = os.path.getsize(temp_file) if os.path.exists(temp_file) else 0
        range_start = start + existing_size
        if range_start > end:
            return i
        headers = {
            "Cookie": f"accountToken={self.token}",
            "Range": f"bytes={range_start}-{end}"
        }
        with requests.get(link, headers=headers, stream=True) as r:
            r.raise_for_status()
            with open(temp_file, "ab") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        with self.progress_lock:
                            self.progress_bar.update(len(chunk))
        return i

    # merge temp files
    def _merge_temp_files(self, temp_dir, dest, num_threads):
        with open(dest, "wb") as outfile:
            for i in range(num_threads):
                temp_file = os.path.join(temp_dir, f"part_{i}")
                with open(temp_file, "rb") as f:
                    outfile.write(f.read())
                os.remove(temp_file)
        shutil.rmtree(temp_dir)

    def download(self, file: File, num_threads=1, use_aria2=False, aria2_args=None, skip_if_exists=True, resume=True, quiet=False):
        link = file.link
        dest = file.dest
        temp_dir = dest + "_parts"
        try:
            # get file size, and if the site supports range
            total_size, is_support_range, final_url = self._get_total_size(link)

            # skip download if the file has been fully downloaded
            if os.path.exists(dest):
                is_aria2_incomplete = use_aria2 and os.path.exists(dest + ".aria2")
                if (resume or skip_if_exists) and not is_aria2_incomplete:
                    # For requests, we check size match if possible
                    if not use_aria2 and total_size > 0 and os.path.getsize(dest) != total_size:
                        pass # proceed
                    else:
                        logger.info(f"Skipping already downloaded file: {dest}")
                        return

            if use_aria2:
                is_aria2_incomplete = os.path.exists(dest + ".aria2")
                success = aria2_download(
                    url=final_url,
                    output=dest,
                    quiet=quiet,
                    proxy=os.environ.get("HTTP_PROXY"),
                    token=self.token,
                    resume=resume or is_aria2_incomplete,
                    aria2_args=aria2_args,
                )
                if success:
                    return
                else:
                    logger.warning("aria2c failed, falling back to internal downloader")

            if num_threads == 1 or not is_support_range:
                temp_file = dest + ".part"

                # calculate downloaded bytes
                downloaded_bytes = os.path.getsize(temp_file) if os.path.exists(temp_file) else 0

                # start progress bar
                if len(os.path.basename(dest)) > 25:
                    display_name = os.path.basename(dest)[:10] + "....." + os.path.basename(dest)[-10:]
                else:
                    display_name = os.path.basename(dest).rjust(25)
                self.progress_bar = tqdm(total=total_size, initial=downloaded_bytes, unit='B', unit_scale=True, desc=f'Downloading {display_name}')

                # download file
                headers = {
                    "Cookie": f"accountToken={self.token}",
                    "Range": f"bytes={downloaded_bytes}-"
                }
                os.makedirs(os.path.dirname(dest), exist_ok=True)
                with requests.get(link, headers=headers, stream=True) as r:
                    r.raise_for_status()
                    with open(temp_file, "ab") as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                                self.progress_bar.update(len(chunk))

                # close progress bar
                self.progress_bar.close()

                # rename temp file
                os.rename(temp_file, dest)
            else:
                # remove single thread file if it exists
                os.path.exists(dest + ".part") and os.remove(dest + ".part")

                # check if the num_threads is matched
                # remove the previous downloaded temp files if it doesn't match
                check_file = os.path.join(temp_dir, "num_threads")
                if os.path.exists(temp_dir):
                    prev_num_threads = None
                    if os.path.exists(check_file):
                        with open(check_file) as f:
                            prev_num_threads = int(f.read())
                    if prev_num_threads is None or prev_num_threads != num_threads:
                        shutil.rmtree(temp_dir)

                if not os.path.exists(temp_dir):
                    # create temp directory for temp files
                    os.makedirs(temp_dir, exist_ok=True)

                    # add check_file
                    with open(check_file, "w") as f:
                        f.write(str(num_threads))

                # calculate the number of temp files
                part_size = math.ceil(total_size / num_threads)

                # calculate downloaded bytes
                downloaded_bytes = 0
                for i in range(num_threads):
                    part_file = os.path.join(temp_dir, f"part_{i}")
                    if os.path.exists(part_file):
                        downloaded_bytes += os.path.getsize(part_file)

                # start progress bar
                if len(os.path.basename(dest)) > 25:
                    display_name = os.path.basename(dest)[:10] + "....." + os.path.basename(dest)[-10:]
                else:
                    display_name = os.path.basename(dest).rjust(25)
                self.progress_bar = tqdm(total=total_size, initial=downloaded_bytes, unit='B', unit_scale=True, desc=f'Downloading {display_name}')

                # download temp files
                futures = []
                with ThreadPoolExecutor(max_workers=num_threads) as executor:
                    for i in range(num_threads):
                        start = i * part_size
                        end = min(start + part_size - 1, total_size - 1)
                        temp_file = os.path.join(temp_dir, f"part_{i}")
                        futures.append(executor.submit(self._download_range, link, start, end, temp_file, i))
                    for future in as_completed(futures):
                        future.result()

                # close progress bar
                self.progress_bar.close()

                # merge temp files
                self._merge_temp_files(temp_dir, dest, num_threads)
        except Exception as e:
            if self.progress_bar:
                self.progress_bar.close()
            logger.error(f"failed to download ({e}): {dest} ({link})")


class GoFileMeta(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


class GoFile(metaclass=GoFileMeta):
    def __init__(self) -> None:
        self.token = ""
        self.wt = ""
        self.lock = Lock()

    def update_token(self) -> None:
        if self.token == "":
            data = requests.post("https://api.gofile.io/accounts").json()
            if data["status"] == "ok":
                self.token = data["data"]["token"]
                logger.info(f"updated token: {self.token}")
            else:
                raise Exception("cannot get token")

    def update_wt(self) -> None:
        if self.wt == "":
            alljs = requests.get("https://gofile.io/dist/js/config.js").text
            if 'appdata.wt = "' in alljs:
                self.wt = alljs.split('appdata.wt = "')[1].split('"')[0]
                logger.info(f"updated wt: {self.wt}")
            else:
                raise Exception("cannot get wt")

    def execute(
        self, 
        dir: str, 
        content_id: str = None, 
        url: str = None, 
        password: str = None, 
        proxy: str = None, 
        num_threads: int = 1, 
        includes: list[str] = None, 
        excludes: list[str] = None,
        use_aria2: bool = False,
        aria2_args: str = None,
        skip_if_exists: bool = True,
        resume: bool = True) -> None:
        if proxy is not None:
            logger.info(f"Proxy set to: {proxy}")
            os.environ['HTTP_PROXY'] = proxy
            os.environ['HTTPS_PROXY'] = proxy
        else:
            os.environ.pop('HTTP_PROXY', None)
            os.environ.pop('HTTPS_PROXY', None)

        files = self.get_files(dir, content_id, url, password, includes, excludes)
        for file in files:
            Downloader(token=self.token).download(
                file, 
                num_threads=num_threads,
                use_aria2=use_aria2,
                aria2_args=aria2_args,
                skip_if_exists=skip_if_exists,
                resume=resume
            )

    def is_included(self, filename: str, includes: list[str]) -> bool:
        if len(includes) == 0:
            return True
        return any(fnmatch.fnmatch(filename, pattern) for pattern in includes)
    
    def is_excluded(self, filename: str, excludes: list[str]) -> bool:
        if len(excludes) == 0:
            return False
        return any(fnmatch.fnmatch(filename, pattern) for pattern in excludes)

    def get_files(
            self, dir: str, 
            content_id: str = None, 
            url: str = None, 
            password: str = None, 
            includes: list[str] = None,
            excludes: list[str] = None) -> list[File]:
        if includes is None:
            includes = []
        if excludes is None:
            excludes = []
        files = list()
        if content_id is not None:
            self.update_token()
            self.update_wt()
            hash_password = hashlib.sha256(password.encode()).hexdigest() if password != None else ""
            data = requests.get(
                f"https://api.gofile.io/contents/{content_id}?cache=true&password={hash_password}",
                headers={
                    "Authorization": "Bearer " + self.token,
                    "X-Website-Token": self.wt,
                },
            ).json()
            if data["status"] == "ok":
                if data["data"].get("passwordStatus", "passwordOk") == "passwordOk":
                    if data["data"]["type"] == "folder":
                        dirname = data["data"]["name"]
                        dir = os.path.join(dir, sanitize_filename(dirname))
                        for (id, child) in data["data"]["children"].items():
                            if child["type"] == "folder":
                                folder_files = self.get_files(dir=dir, content_id=id, password=password, includes=includes, excludes=excludes)
                                files.extend(folder_files)
                            else:
                                filename = child["name"]
                                if self.is_included(filename, includes) and not self.is_excluded(filename, excludes):
                                    files.append(File(
                                        link=child["link"],
                                        dest=os.path.join(dir, sanitize_filename(filename))))
                    else:
                        filename = data["data"]["name"]
                        if self.is_included(filename, includes) and not self.is_excluded(filename, excludes):
                            files.append(File(
                                link=data["data"]["link"],
                                dest=os.path.join(dir, sanitize_filename(filename))))
                else:
                    logger.error(f"invalid password: {data['data'].get('passwordStatus')}")
        elif url is not None:
            if url.startswith("https://gofile.io/d/"):
                files = self.get_files(dir=dir, content_id=url.split("/")[-1], password=password, includes=includes, excludes=excludes)
            else:
                logger.error(f"invalid url: {url}")
        else:
            logger.error(f"invalid parameters")
        return files

def signal_handler(sig, frame):
    logger.info("Interrupt received, exiting...")
    os._exit(1)


def run_main_logic():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("url", nargs='?', default=None, help="url to process (if not using -f)")
    group.add_argument("-f", type=str, dest="file", help="local file to process")
    parser.add_argument("-t", type=int, dest="num_threads", help="number of threads (default: 1)")
    parser.add_argument("-d", type=str, dest="dir", help="output directory")
    parser.add_argument("-p", type=str, dest="password", help="password")
    parser.add_argument("-x", type=str, dest="proxy", help="proxy server (format: ip/host:port)")
    parser.add_argument("-i", action="append", dest="includes", help="included files (supporting wildcard *)")
    parser.add_argument("-e", action="append", dest="excludes", help="excluded files (supporting wildcard *)")
    
    parser.add_argument(
        "--aria2c",
        action="store_true",
        help="use aria2c for download",
    )
    parser.add_argument(
        "--aria2c-args",
        help="additional arguments for aria2c",
    )
    parser.add_argument(
        "--skip-if-exists",
        action="store_true",
        default=True,
        help="skip download if file already exists (default: True)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="overwrite existing file if it already exists",
    )
    parser.add_argument(
        "--continue",
        "-c",
        dest="continue_",
        action="store_true",
        default=True,
        help="resume getting partially-downloaded files (default: True)",
    )

    args = parser.parse_args()
    num_threads = args.num_threads if args.num_threads is not None else 1
    dir = args.dir if args.dir is not None else "./output"

    skip_if_exists = args.skip_if_exists and not args.overwrite
    resume = args.continue_ and not args.overwrite

    if args.file is not None:
        if os.path.exists(args.file):
            with open(args.file) as f:
                for line in f:
                    line = line.strip()
                    if line == "" or line.startswith("#"):
                        continue
                    GoFile().execute(
                        dir=dir, 
                        url=line, 
                        password=args.password, 
                        proxy=args.proxy, 
                        num_threads=num_threads, 
                        includes=args.includes, 
                        excludes=args.excludes,
                        use_aria2=args.aria2c,
                        aria2_args=args.aria2c_args,
                        skip_if_exists=skip_if_exists,
                        resume=resume)
        else:
            logger.error(f"file not found: {args.file}")
    else:
        GoFile().execute(
            dir=dir, 
            url=args.url, 
            password=args.password, 
            proxy=args.proxy, 
            num_threads=num_threads, 
            includes=args.includes, 
            excludes=args.excludes,
            use_aria2=args.aria2c,
            aria2_args=args.aria2c_args,
            skip_if_exists=skip_if_exists,
            resume=resume)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    
    # Run in a daemon thread for responsive interruption
    main_thread = threading.Thread(target=run_main_logic)
    main_thread.daemon = True
    main_thread.start()
    
    while main_thread.is_alive():
        main_thread.join(timeout=0.1)
