import os
import sys
import time
import threading
import pytest
import cv2
import numpy as np
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from visualize_land import generate_map

class SeleniumRecorder(threading.Thread):
    """Background thread to capture screenshots and encode them into a video."""
    def __init__(self, driver, output_path="selenium_test_screencast.mp4", fps=5):
        super().__init__()
        self.driver = driver
        self.output_path = output_path
        self.fps = fps
        self.stop_event = threading.Event()
        self.frames = []

    def run(self):
        while not self.stop_event.is_set():
            try:
                # Capture the current browser state
                screenshot = self.driver.get_screenshot_as_png()
                nparr = np.frombuffer(screenshot, np.uint8)
                img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
                self.frames.append(img)
                time.sleep(1.0 / self.fps)
            except Exception:
                break

    def stop(self):
        self.stop_event.set()
        self.join()
        if self.frames:
            height, width, _ = self.frames[0].shape
            fourcc = cv2.VideoWriter_fourcc(*'mp4v')
            video = cv2.VideoWriter(self.output_path, fourcc, self.fps, (width, height))
            for frame in self.frames:
                video.write(frame)
            video.release()
            print(f"\n🎬 Screencast saved to {self.output_path}")

@pytest.fixture(scope="module")
def setup_teardown_driver(tmp_path_factory):
    # Set env vars to point to test data
    os.environ["CSV_FILE"] = os.path.join(os.path.dirname(__file__), "backbone_locations.csv")
    os.environ["STRATEGIC_FILE"] = os.path.join(os.path.dirname(__file__), "strategic_analysis.json")

    # Setup WebDriver
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080") # Fixed size for consistent video
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    # Start the recorder
    recorder = SeleniumRecorder(driver)
    recorder.start()

    # Generate the map
    output_dir = tmp_path_factory.mktemp("html_output")
    html_file = os.path.join(output_dir, "index.html")
    
    # Defensive: Handle both original and updated generate_map signatures
    try:
        generate_map(output_file=html_file)
    except TypeError:
        generate_map()
        if os.path.exists("index.html"):
            os.rename("index.html", html_file)
            
    driver.get("file://" + html_file)

    yield driver

    # Stop recording and cleanup
    recorder.stop()
    driver.quit()

def test_filters_work_without_nan(setup_teardown_driver):
    driver = setup_teardown_driver
    wait = WebDriverWait(driver, 10)
    wait.until(EC.presence_of_element_located((By.ID, "agg-count")))
    time.sleep(2) 

    initial_count = driver.find_element(By.ID, "agg-count").text
    assert initial_count != "0"

    apply_button = driver.find_element(By.XPATH, "//button[text()='Apply Filters']")
    apply_button.click()
    time.sleep(1)

    final_count = driver.find_element(By.ID, "agg-count").text
    assert initial_count == final_count

def test_filters_reduce_count_successfully(setup_teardown_driver):
    driver = setup_teardown_driver
    wait = WebDriverWait(driver, 10)

    wait.until(EC.presence_of_element_located((By.ID, "agg-count")))
    initial_count_text = driver.find_element(By.ID, "agg-count").text
    initial_count = int(initial_count_text)

    select_element = driver.find_element(By.ID, "sel-type")
    select = Select(select_element)
    select.deselect_all()
    select.select_by_value("Camping")

    driver.find_element(By.XPATH, "//button[text()='Apply Filters']").click()

    wait.until(lambda d: d.find_element(By.ID, "agg-count").text != initial_count_text)
    final_count = int(driver.find_element(By.ID, "agg-count").text)

    assert final_count < initial_count
    assert final_count > 0
