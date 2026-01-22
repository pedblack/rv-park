import os

# Import the generate_map function from the parent directory
import sys
import time

import pytest
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from visualize_land import generate_map


@pytest.fixture(scope="module")
def setup_teardown_driver(tmp_path_factory):
    # Set env vars to point to test data
    os.environ["CSV_FILE"] = os.path.join(
        os.path.dirname(__file__), "backbone_locations.csv"
    )
    os.environ["STRATEGIC_FILE"] = os.path.join(
        os.path.dirname(__file__), "strategic_analysis.json"
    )

    # Setup WebDriver
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(options=options)

    # Generate the map
    output_dir = tmp_path_factory.mktemp("html_output")
    html_file = os.path.join(output_dir, "index.html")
    generate_map(output_file=html_file)
    driver.get("file://" + html_file)

    yield driver

    # Teardown WebDriver
    driver.quit()


def test_filters_work_without_nan(setup_teardown_driver):
    driver = setup_teardown_driver
    # Wait for map and JS to initialize
    wait = WebDriverWait(driver, 10)
    wait.until(EC.presence_of_element_located((By.ID, "agg-count")))
    time.sleep(2)  # Wait for JS timeout to run and populate dashboard

    initial_count_element = driver.find_element(By.ID, "agg-count")
    initial_count = initial_count_element.text
    assert initial_count != "0", "Map should have markers on load"

    # Find and click apply button
    apply_button = driver.find_element(By.XPATH, "//button[text()='Apply Filters']")
    apply_button.click()

    # Wait for filtering to apply
    time.sleep(1)

    # After clicking apply with default values, count should still be the same
    final_count = driver.find_element(By.ID, "agg-count").text
    assert (
        initial_count == final_count
    ), "Filtering with default values should not remove all markers. Check for NaN issues."


def test_filters_reduce_count_successfully(setup_teardown_driver):
    driver = setup_teardown_driver
    wait = WebDriverWait(driver, 10)

    # 1. Wait for map to load and get the starting count
    wait.until(EC.presence_of_element_located((By.ID, "agg-count")))
    initial_count_element = driver.find_element(By.ID, "agg-count")
    initial_count_text = initial_count_element.text
    initial_count = int(initial_count_text)

    # Sanity check: ensure we have data to begin with
    assert initial_count > 0, "Map should have markers on load"

    # 2. Interact with the "Property Types" filter and select "Camping"
    select_element = driver.find_element(By.ID, "sel-type")
    select = Select(select_element)
    select.deselect_all()
    select.select_by_value("Camping")

    # 3. Click the Apply button
    apply_button = driver.find_element(By.XPATH, "//button[text()='Apply Filters']")
    apply_button.click()

    # 4. Use a custom wait to see when the text changes from the initial value
    wait.until(lambda d: d.find_element(By.ID, "agg-count").text != initial_count_text)

    # 5. Final Assertions
    final_count = int(driver.find_element(By.ID, "agg-count").text)

    # Ensure it filtered 'some' but not 'all'
    assert (
        final_count < initial_count
    ), f"Filter failed to reduce count. Started with {initial_count}, still have {final_count}."
    assert (
        final_count > 0
    ), "Filter was too aggressive and removed all markers (possible NaN or logic error)."
