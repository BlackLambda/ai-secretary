from datetime import datetime
from enum import Enum
import json
import logging
import os
from pathlib import Path
import subprocess
import time
import traceback

import requests
from playwright.sync_api import sync_playwright


class VideoFileStatus(Enum):
    UNKNOWN = 1
    DOWNLOADING = 2
    DONE = 3
    FAILED = 4
    NO_ACCESS = 5

class VideoService:
    """
    Service for downloading teams meeting recording videos using Playwright to handle authentication and cookies.
    """

    def __init__(self):
        self.result = {}
        self.log_format = "{timestamp} - VideoService - {message}"
        self.output_path = Path(__file__).parent.parent.parent / "output"
        self.video_input_path = self.output_path / "daily"
        self.video_output_path = self.output_path / "videos"
        os.makedirs(str(self.video_output_path), exist_ok=True)

    def run(self):
        # Get video URLs from transcripts
        video_urls = self.get_video_urls()
        # Get current videos
        existing_videos = set(os.listdir(self.video_output_path))
        # Open Browser with remote debugging
        self.open_browser()
        # Download videos
        video_urls_to_download = [url for url in video_urls if url.rsplit('/', 1)[-1] not in existing_videos]
        self.download_videos(video_urls_to_download)
        # Report summary
        self.report()

    def get_video_urls(self):
        video_urls = []
        for file in os.listdir(self.video_input_path):
            if file.endswith("_meeting_transcripts.json"):
                with open(self.video_input_path / file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    for meeting in data["transcripts_list"]:
                        video_url = meeting["sharepoint_item"]["FileUrl"]
                        video_urls.append(video_url)
                        logging.info(f"Found video URL: {video_url}")
        return  video_urls

    def download_videos(self, video_urls):
        logging.info(f"Starting to download {len(video_urls)} videos...")
        with sync_playwright() as p:
            browser = p.chromium.connect_over_cdp("http://localhost:9222")
            if browser.contexts:
                context = browser.contexts[0]
            else:
                context = browser.new_context()
            if context.pages:
                page = context.pages[0]
            else:
                page = context.new_page()

            for url in video_urls:
                filename = url.rsplit('/', 1)[-1]
                self.result[filename] = VideoFileStatus.UNKNOWN
                        
                try:
                    page.goto(url)
                    page.wait_for_timeout(5000)

                    if "login" in page.url:
                        input("Login in browser, then press Enter to continue...")
                    elif page.get_by_text("You need access").is_visible() or \
                        page.get_by_text("Sorry, you don't have access.").is_visible():
                        logging.info(f"You don't have access to {url}")
                        self.result[filename] = VideoFileStatus.NO_ACCESS
                        continue

                    self.result[filename] = VideoFileStatus.DOWNLOADING
                    cookies = context.cookies()
                    logging.info(f"Downloading from {url}...")
                    response = requests.get(url, stream=True, cookies={c['name']: c['value'] for c in cookies if c["domain"] in url})
                    response.raise_for_status()
                    
                    with open(self.video_output_path / filename, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)

                    self.result[filename] = VideoFileStatus.DONE

                except Exception as e:
                    self.result[filename] = VideoFileStatus.FAILED
                    logging.error(f"Error during download: {e}")
                    traceback.print_exception(e)

            page.close()
            context.close()
            browser.close()

    def report(self):
        logging.info("Video Download Summary:")
        for status in (
            VideoFileStatus.DONE,
            VideoFileStatus.FAILED,
            VideoFileStatus.NO_ACCESS,
            VideoFileStatus.DOWNLOADING,
            VideoFileStatus.UNKNOWN
        ):
            count = sum(1 for s in self.result.values() if s == status)
            if count == 0:
                continue
            logging.info(f"{status.name}: {count} videos:")
            for f, s in self.result.items():
                if status == s:
                    logging.info(f)

    @staticmethod
    def open_browser(profile='C:\\chrome-dev-profile', port=9222):
        chrome_exe = 'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe'
        edge_exe = "C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe"
        if os.path.exists(edge_exe):
            cmd = edge_exe
        elif os.path.exists(chrome_exe):
            cmd = chrome_exe
        else:
            logging.error("Neither Chrome nor Edge browser found.")
            raise RuntimeError("Neither Chrome nor Edge browser found.")
        args = f'--remote-debugging-port={port} --user-data-dir={profile} --window-size=1280,1280'
        command = f'"{cmd}" {args}'
        subprocess.Popen(command)
        time.sleep(3)
