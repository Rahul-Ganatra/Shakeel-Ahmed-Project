import requests
from bs4 import BeautifulSoup
import json
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from concurrent.futures import ThreadPoolExecutor, as_completed

URL = "https://identifier.buildingsmart.org/uri/buildingsmart/ifc/4.3/class/IfcRoot"

def fetch_html(url):
    options = Options()
    options.add_argument("--headless")
    driver = webdriver.Chrome(options=options)
    driver.get(url)
    try:
        # Expand the "Properties" accordion if it exists
        try:
            accordion = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, "//div[contains(text(), 'Properties')]"))
            )
            driver.execute_script("arguments[0].click();", accordion)
        except Exception:
            pass  # It's ok if not found

        # Expand the "Incoming relations" accordion if it exists
        try:
            incoming_accordion = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, "//div[contains(text(), 'Incoming relations')]"))
            )
            driver.execute_script("arguments[0].click();", incoming_accordion)
            # Wait for the table under "Incoming relations" to appear
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//div[contains(text(), 'Incoming relations')]/ancestor::div[contains(@class, 'bsdd-title-sub')]/following-sibling::table"))
            )
        except Exception as e:
            print("Warning: Could not expand 'Incoming relations' or table did not load.", e)

        # Wait for any properties table to appear (for completeness)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "td.mat-column-name"))
        )
    except Exception as e:
        print("Warning: Could not expand accordions or table did not load.", e)
    html = driver.page_source
    driver.quit()
    return html

def get_section_by_heading(soup, heading_texts):
    """
    Find a section by heading text (h2, h3, etc.) and return the next sibling (usually a div or table).
    heading_texts: list of possible heading names (case-insensitive)
    """
    for heading in soup.find_all(['h2', 'h3', 'h4']):
        if any(h.lower() in heading.text.lower() for h in heading_texts):
            # Return the next sibling that is a tag (skip NavigableString)
            sib = heading.find_next_sibling()
            while sib and not hasattr(sib, 'name'):
                sib = sib.find_next_sibling()
            return sib
    return None

def get_properties_after_heading(soup, heading_texts):
    """
    Find all property names (accordion buttons) after a heading matching one of heading_texts.
    """
    for heading in soup.find_all(['h2', 'h3', 'h4']):
        if any(h.lower() in heading.text.lower() for h in heading_texts):
            # Find all buttons after this heading in the document order
            properties = []
            found_heading = False
            for tag in soup.find_all(True):
                if tag == heading:
                    found_heading = True
                elif found_heading and tag.name == 'button' and 'accordion-button' in tag.get('class', []):
                    prop_name = tag.text.strip()
                    if prop_name:
                        properties.append(prop_name)
            return properties
    return []

def extract_relations(soup, section_titles):
    """
    Extract relations from a section with a given heading (e.g., 'Relations', 'Incoming Relations').
    Returns a list of relation names.
    """
    relations = []

    # Try to find the section by heading and get the next table
    for div in soup.find_all('div', class_='bsdd-title-sub'):
        if any(title.lower() in div.text.lower() for title in section_titles) or "incoming" in div.text.lower():
            table = div.find_next('table')
            if table:
                for row in table.find_all('tr'):
                    name_td = row.find('td', class_='mat-column-name')
                    if name_td:
                        a = name_td.find('a')
                        if a:
                            relations.append(a.text.strip())
                        elif name_td.text.strip():
                            relations.append(name_td.text.strip())
            if relations:
                return relations

    # Fallback: search all tables for a header with "incoming"
    for table in soup.find_all('table'):
        thead = table.find('thead')
        if thead and any("incoming" in th.text.lower() for th in thead.find_all('th')):
            for row in table.find_all('tr'):
                name_td = row.find('td', class_='mat-column-name')
                if name_td:
                    a = name_td.find('a')
                    if a:
                        relations.append(a.text.strip())
                    elif name_td.text.strip():
                        relations.append(name_td.text.strip())
            if relations:
                return relations

    return relations

def extract_incoming_relations(soup):
    """
    Extract all incoming relations from the 'Incoming relations' table.
    Returns a list of dicts with all relevant details.
    """
    incoming_relations = []
    for div in soup.find_all('div', class_='bsdd-title-sub'):
        if "incoming" in div.text.lower():
            table = div.find_next('table')
            if table:
                for row in table.find_all('tr'):
                    # Skip header rows
                    if row.find('th'):
                        continue
                    tds = row.find_all('td')
                    if len(tds) < 6:
                        continue
                    relates_with = tds[0].get_text(strip=True)
                    direction = tds[1].get_text(strip=True)
                    uri = ""
                    uri_a = tds[2].find('a')
                    if uri_a:
                        uri = uri_a.get('href', '').strip()
                    rel_type = tds[3].get_text(strip=True)
                    dictionary = tds[4].get_text(strip=True)
                    version_status = tds[5].get_text(strip=True)
                    incoming_relations.append({
                        "relates_with": relates_with,
                        "direction": direction,
                        "uri": uri,
                        "type": rel_type,
                        "dictionary": dictionary,
                        "version_status": version_status
                    })
            break
    return incoming_relations

def extract_table_rows_by_heading(soup, heading_keyword):
    """
    Extracts rows from the first table found after a heading div containing heading_keyword.
    Returns a list of dicts with name, data_type, and definition.
    """
    results = []
    for div in soup.find_all('div', class_='bsdd-title-sub'):
        if heading_keyword.lower() in div.text.lower():
            table = div.find_next('table')
            if table:
                for row in table.find_all('tr'):
                    name_td = row.find('td', class_='mat-column-name')
                    datatype_td = row.find('td', class_='mat-column-dataType')
                    definition_td = row.find('td', class_='mat-column-definition')
                    if name_td:
                        name = name_td.get_text(strip=True)
                        datatype = datatype_td.get_text(strip=True) if datatype_td else ""
                        definition = definition_td.get_text(strip=True) if definition_td else ""
                        results.append({
                            "name": name,
                            "data_type": datatype,
                            "definition": definition
                        })
            break
    return results

def extract_all_properties(driver):
    """
    Use Selenium to extract all properties from all pages of the properties table.
    Returns a list of dicts with all relevant details.
    """
    properties = []
    while True:
        # Parse the current page
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        for div in soup.find_all('div', class_='bsdd-title-sub'):
            if "properties" in div.text.lower():
                table = div.find_next('table')
                if table:
                    for row in table.find_all('tr'):
                        if row.find('th'):
                            continue
                        tds = row.find_all('td')
                        if len(tds) < 3:
                            continue
                        name = tds[0].get_text(strip=True)
                        data_type = tds[1].get_text(strip=True)
                        definition = tds[3].get_text(strip=True) if len(tds) > 3 else ""
                        properties.append({
                            "name": name,
                            "data_type": data_type,
                            "definition": definition
                        })
                break
        # Try to click the "next page" button
        try:
            next_button = driver.find_element(By.CSS_SELECTOR, "button.mat-mdc-paginator-navigation-next:not([disabled])")
            driver.execute_script("arguments[0].click();", next_button)
            time.sleep(1)  # Wait for the next page to load
        except Exception:
            break  # No more pages
    return properties

def parse_html(html):
    soup = BeautifulSoup(html, 'html.parser')

    # 1. Class Name (from the bsdd-title div)
    class_name_div = soup.find('div', class_='bsdd-title')
    class_name = class_name_div.text.strip() if class_name_div else "Unknown"

    # 2. Code (from the span after "Code" label)
    code = None
    for field in soup.find_all('app-bsdd-field-value'):
        label = field.find('span', string="Code")
        if label:
            code_span = field.find('span', class_='ng-star-inserted')
            if code_span:
                code = code_span.text.strip()
                break
    if not code:
        code = class_name  # fallback

    # 4. Child Classes (from the Child classes section)
    child_classes = []
    for field in soup.find_all('app-bsdd-field-uris-list'):
        label = field.find('span', string="Child classes")
        if label:
            for a in field.find_all('a', href=True):
                child_classes.append(a['href'])

    # 5. Outgoing Relations (from the Relations section)
    relations = extract_relations(soup, ['Relations'])

    # 6. Incoming Relations (from the Incoming Relations section)
    incoming_relations = extract_incoming_relations(soup)

    return {
        "class_name": class_name,
        "code": code,
        "child_classes": child_classes,
        "relations": relations,
        "incoming_relations": incoming_relations
    }

def crawl_one_class(url):
    """
    Crawl a single class page and return its data and child class URLs.
    """
    options = Options()
    options.add_argument("--headless")
    driver = webdriver.Chrome(options=options)
    driver.get(url)
    # Expand accordions as before
    try:
        accordion = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.XPATH, "//div[contains(text(), 'Properties')]"))
        )
        driver.execute_script("arguments[0].click();", accordion)
    except Exception:
        pass
    try:
        incoming_accordion = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.XPATH, "//div[contains(text(), 'Incoming relations')]"))
        )
        driver.execute_script("arguments[0].click();", incoming_accordion)
    except Exception:
        pass
    properties = extract_all_properties(driver)
    html = driver.page_source
    driver.quit()
    data = parse_html(html)
    data["url"] = url
    data["properties"] = properties

    # Convert relative child URLs to absolute
    child_urls = []
    for child_url in data.get("child_classes", []):
        if child_url.startswith("/"):
            child_url = "https://identifier.buildingsmart.org" + child_url
        child_urls.append(child_url)
    return data, child_urls

def crawl_all_classes(start_url):
    """
    Recursively crawl all classes using multithreading.
    """
    visited = set()
    results = []
    to_crawl = [start_url]

    with ThreadPoolExecutor(max_workers=8) as executor:  # Adjust max_workers as per your CPU/RAM
        while to_crawl:
            # Only crawl URLs not yet visited
            futures = {executor.submit(crawl_one_class, url): url for url in to_crawl if url not in visited}
            to_crawl = []
            for future in as_completed(futures):
                url = futures[future]
                try:
                    data, child_urls = future.result()
                    results.append(data)
                    visited.add(url)
                    for child_url in child_urls:
                        if child_url not in visited:
                            to_crawl.append(child_url)
                except Exception as e:
                    print(f"Error processing {url}: {e}")
    return results

def main():
    BASE_URL = "https://identifier.buildingsmart.org/uri/buildingsmart/ifc/4.3/class/IfcRoot"
    all_data = crawl_all_classes(BASE_URL)
    with open("all_classes_threaded.json", "w", encoding="utf-8") as f:
        json.dump(all_data, f, indent=2, ensure_ascii=False)
    print(f"Saved data for {len(all_data)} classes.")

def get_all_class_urls(base_url):
    """
    Fetch all class URLs from the base class listing page.
    """
    html = requests.get(base_url).text
    soup = BeautifulSoup(html, 'html.parser')
    class_urls = set()
    for a in soup.find_all('a', href=True):
        href = a['href']
        # Only keep links to class pages (adjust pattern as needed)
        if '/class/' in href and href.startswith('/uri/'):
            class_urls.add('https://identifier.buildingsmart.org' + href)
    return list(class_urls)

if __name__ == "__main__":
    main()
