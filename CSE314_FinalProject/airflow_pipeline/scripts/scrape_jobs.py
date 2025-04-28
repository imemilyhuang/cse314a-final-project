import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

def slugify(name):
    return re.sub(r"[^a-z0-9]+", "-", name.lower().strip()).strip("-")

def simplifyify(name):
    return name.strip().replace(" ", "-")

def scrape_internships():
    options = webdriver.ChromeOptions()
    options.add_argument('--ignore-ssl-errors=yes')
    options.add_argument('--ignore-certificate-errors')

    user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36'
    options.add_argument(f'user-agent={user_agent}')
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("-headless")

    remote_webdriver = 'remote_chromedriver'
    with webdriver.Remote(f'{remote_webdriver}:4444/wd/hub', options=options) as driver:
    
        driver.get("https://www.levels.fyi/internships/")
        time.sleep(5)
        
        soup = BeautifulSoup(driver.page_source, "html.parser")
        driver.quit()

        rows = soup.find_all("tr", attrs={"data-index": True})

        internships = []

        for row in rows:
            try:
                company_name = row.select_one(".company-info-cell h6")
                company_name = company_name.get_text(strip=True) if company_name else ""

                location_season = row.select_one(".company-info-cell p")
                location_season = location_season.get_text(strip=True) if location_season else ""

                title = row.select_one("span.badge")
                title = title.get_text(strip=True) if title else ""

                hourly = row.select_one(".hourly-salary-td h6 span.cashInText")
                hourly = hourly.get_text(strip=True) if hourly else ""

                monthly = row.select_one(".hourly-salary-td p.text-muted span.cashInText")
                monthly = monthly.get_text(strip=True) if monthly else ""

                perk_tags = row.select(".tags-th p.cashInText")
                perks = "\n".join(p.get_text(strip=True) for p in perk_tags)

                apply_link_tag = row.select_one("a[href]")
                apply_link = apply_link_tag["href"] if apply_link_tag else ""

                company_slug = slugify(company_name)

                internships.append({
                    "company_slug": company_slug,
                    "company_name": company_name,
                    "title": title,
                    "location": location_season,
                    "hourly_rate": hourly,
                    "monthly_pay": monthly,
                    "perks": perks,
                    "apply_link": apply_link
                })

            except Exception as e:
                print(f"Error parsing internship row: {e}")
                continue

        print(f"Successfully scraped {len(internships)} internships")
        return pd.DataFrame(internships)

def get_company_info(slug):
    url = f"https://www.levels.fyi/companies/{slug}"
    print(f"Scraping Levels.fyi: {url}")
    try:
        res = requests.get(url, timeout=10)
        soup = BeautifulSoup(res.text, "html.parser")

        def extract_text(selector):
            el = soup.select_one(selector)
            return el.get_text(strip=True) if el else ""

        def extract_caption_value(label):
            span = soup.find("span", text=label)
            if span:
                h6 = span.find_previous_sibling("h6")
                return h6.get_text(strip=True) if h6 else ""
            return ""

        description = extract_text(".company-page_companyDescription__JVjrt > p")
        links = soup.select("h6.MuiTypography-subtitle1 a[href]")
        website = links[0]["href"] if links else ""
        twitter, linkedin = "", ""
        for a in links[1:]:
            href = a["href"]
            if "twitter.com" in href:
                twitter = href
            elif "linkedin.com" in href:
                linkedin = href

        year_founded = extract_caption_value("Year Founded")
        num_employees = extract_caption_value("# of Employees")

        iframe = soup.find("iframe", {"title": "Company Address"})
        address = ""
        if iframe and "q=" in iframe.get("src", ""):
            match = re.search(r"q=([^&]+)", iframe["src"])
            if match:
                address = match.group(1).replace("%20", " ").replace(",", ", ")

        return {
            "company_slug": slug,
            "description": description,
            "website": website,
            "twitter": twitter,
            "linkedin": linkedin,
            "year_founded": year_founded,
            "num_employees": num_employees,
            "headquarters": address
        }

    except Exception as e:
        print(f"Failed to scrape Levels.fyi company page: {e}")
        return {
            "company_slug": slug,
            "description": "",
            "website": "",
            "twitter": "",
            "linkedin": "",
            "year_founded": "",
            "num_employees": "",
            "headquarters": ""
        }

def get_simplify_company_profile(name):
    slug = simplifyify(name)
    url = f"https://simplify.jobs/c/{slug}"
    print(f"Scraping detailed Simplify profile: {url}")

    try:
        res = requests.get(url, timeout=10)
        soup = BeautifulSoup(res.text, "html.parser")

        def extract_section_text(header_text):
            header = soup.find(lambda tag: tag.name in ["h2", "h3", "h4", "h5"] and header_text.lower() in tag.text.lower())
            content = []
            if header:
                for sibling in header.find_next_siblings():
                    if sibling.name in ["h2", "h3", "h4", "h5"]:
                        break
                    if sibling.name in ["p", "li", "div"]:
                        content.append(sibling.get_text(strip=True))
            return "\n".join(content)

        def extract_rating(label):
            span = soup.find("span", string=re.compile(label, re.I))
            if span and span.find_previous("h5"):
                return span.find_previous("h5").get_text(strip=True)
            return ""

        simplify_take = extract_section_text("Simplify's Take")
        believers = extract_section_text("What believers are saying")
        critics = extract_section_text("What critics are saying")
        uniqueness = extract_section_text("What makes")
        benefits = extract_section_text("Benefits")
        about = extract_section_text("About")

        overall_rating = extract_rating("Simplify's Rating")
        competitive_edge = extract_rating("Competitive Edge")
        growth_potential = extract_rating("Growth Potential")
        rating_diff = extract_rating("Rating Differentiation")

        return {
            "company_name": name,
            "company_simplify_slug": slug,
            "simplify_url": url,
            "simplify_take": simplify_take,
            "believer_points": believers,
            "critic_points": critics,
            "what_makes_unique": uniqueness,
            "benefits": benefits,
            "about_text": about,
            "simplify_rating": overall_rating,
            "competitive_edge": competitive_edge,
            "growth_potential": growth_potential,
            "rating_differentiation": rating_diff
        }

    except Exception as e:
        print(f"Error scraping {url}: {e}")
        return {
            "company_name": name,
            "company_simplify_slug": slug,
            "simplify_url": url,
            "simplify_take": "",
            "believer_points": "",
            "critic_points": "",
            "what_makes_unique": "",
            "benefits": "",
            "about_text": "",
            "simplify_rating": "",
            "competitive_edge": "",
            "growth_potential": "",
            "rating_differentiation": ""
        }

# --- Snowflake upload function ---

def upload_dataframe_to_snowflake(df, table_name):
    conn = snowflake.connector.connect(
        user="MALLARD",
        password="L1lysunsk@ter@gmail.com",
        account="IOB55870",
        warehouse="MALLARD_WH",
        database="MALLARD_DB",
        schema="FINAL_PROJECT"
    )
    try:
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=table_name,
            auto_create_table=True
        )
        if success:
            print(f"Uploaded {nrows} rows to {table_name}!")
        else:
            print(f"Upload failed.")
    finally:
        conn.close()

def scrape_clean_upload():
    print("Scraping internship listings...")
    df_interns = scrape_internships()
    upload_dataframe_to_snowflake(df_interns, "INTERNHIPS")

    print("\nScraping Levels.fyi company profiles...")
    unique_slugs = df_interns["company_slug"].dropna().unique()
    companies = [get_company_info(slug) for slug in unique_slugs]
    df_companies = pd.DataFrame(companies)
    upload_dataframe_to_snowflake(df_companies, "COMPANIES")

    print("\nScraping Simplify detailed profiles...")
    simplify_profiles = [get_simplify_company_profile(name) for name in df_interns["company_name"].dropna().unique()]
    df_simplify = pd.DataFrame(simplify_profiles)
    upload_dataframe_to_snowflake(df_simplify, "SIMPLIFY_COMPANY_PROFILES")
