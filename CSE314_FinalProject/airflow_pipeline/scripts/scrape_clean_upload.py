import re
import pandas as pd

from scripts.scrape_jobs import (
    scrape_internships,
    get_company_info,
    get_simplify_company_profile
)

from scripts.clean_jobs_data import (
    clean_internships,
    clean_companies,
    clean_simplify_profiles,
    build_locations_table,
    build_internship_locations_table,
    build_industries_table,
    build_company_industries_table,
)

from utils.snowflake_utils import upload_dataframe_to_snowflake

def slugify(name):
    return re.sub(r"[^a-z0-9]+", "-", name.lower().strip()).strip("-")\

def normalize_slug(slug):
    if pd.isna(slug):
        return ""
    return re.sub(r"[^a-z0-9]", "", slug.lower())

def scrape_data():
    df_interns = scrape_internships()
    df_interns.to_csv("/tmp/internships_raw.csv", index=False)
    
    company_slugs = df_interns["company_slug"].dropna().unique()
    company_data = [get_company_info(slug) for slug in company_slugs]
    pd.DataFrame(company_data).to_csv("/tmp/companies_raw.csv", index=False)
    
    simplify_profiles = [get_simplify_company_profile(name) for name in df_interns["company_name"].dropna().unique()]
    pd.DataFrame(simplify_profiles).to_csv("/tmp/simplify_raw.csv", index=False)

def clean_data():
    df_levels     = pd.read_csv("/tmp/companies_raw.csv")
    df_interns    = pd.read_csv("/tmp/internships_raw.csv")
    df_simplify   = pd.read_csv("/tmp/simplify_raw.csv")

    df_levels     = clean_companies(df_levels)
    df_interns    = clean_internships(df_interns)
    df_simplify   = clean_simplify_profiles(df_simplify)

    df_levels    ["normalized_slug"] = df_levels    ["company_slug"].apply(normalize_slug)
    df_interns   ["normalized_slug"] = df_interns   ["company_slug"].apply(normalize_slug)
    df_simplify  ["normalized_slug"] = df_simplify  ["company_simplify_slug"].apply(normalize_slug)

    companies = pd.merge(
        df_levels, df_simplify,
        on="normalized_slug",
        how="outer",
        suffixes=("_levels","_simplify")
    )

    companies_table = companies[[
        "normalized_slug",
        "company_name", "description", "overview",
        "website", "twitter", "linkedin",
        "year_founded", "founded_year",
        "num_employees", "company_size",
        "headquarters", "simplify_headquarters",
        "company_stage", "total_funding",
        "simplify_url", "simplify_take",
        "believer_points", "critic_points", "what_makes_unique",
        "benefits", "industries"
    ]].drop_duplicates(subset=["normalized_slug"]).reset_index(drop=True)
    companies_table["company_id"] = companies_table.index + 1

    industry_set = set()
    company_industries = []
    for _, row in companies.iterrows():
        raw = row.get("industries","")
        if pd.notna(raw):
            for ind in [i.strip() for i in raw.split(",")]:
                industry_set.add(ind)
                company_industries.append((row["normalized_slug"], ind))

    industries_table = pd.DataFrame({"name": sorted(industry_set)})
    industries_table["industry_id"] = industries_table.index + 1

    company_industries_table = (
        pd.DataFrame(company_industries, columns=["normalized_slug","industry_name"])
          .merge(companies_table[["normalized_slug","company_id"]], on="normalized_slug")
          .merge(industries_table, left_on="industry_name", right_on="name")
          [["company_id","industry_id"]]
    )

    internships_table = (
        pd.merge(
            df_interns,
            companies_table[["normalized_slug","company_id"]],
            on="normalized_slug", how="left"
        )
        [[
            "company_id","title","location",
            "hourly_rate","monthly_pay",
            "degree_requirement","perks_clean","apply_link"
        ]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    internships_table["internship_id"] = internships_table.index + 1

    location_set = set()
    internship_locations = []
    for _, row in internships_table.iterrows():
        loc = row["location"]
        if pd.notna(loc) and isinstance(loc, str):
            loc_clean = loc.split(" - ")[0].strip()
            parts = [x.strip() for x in re.split(r",|\n", loc_clean)]
            if len(parts)==3:
                city, state, country = parts
            elif len(parts)==2:
                city, state = parts; country=""
            else:
                city=parts[0]; state=country=""
            location_set.add((city, state, country))
            internship_locations.append((row["internship_id"], city, state, country))

    locations_table = pd.DataFrame(
        list(location_set),
        columns=["city","state","country"]
    )
    locations_table["location_id"] = locations_table.index + 1

    internship_locations_table = (
        pd.DataFrame(internship_locations, columns=["internship_id","city","state","country"])
          .merge(locations_table, on=["city","state","country"], how="left")
          [["internship_id","location_id"]]
    )
    internship_locations_table["is_remote"] = False

    internships_table.to_csv("/tmp/export_internships.csv",            index=False)
    companies_table.to_csv(  "/tmp/export_companies.csv",              index=False)
    locations_table.to_csv(  "/tmp/export_locations.csv",              index=False)
    internship_locations_table.to_csv("/tmp/export_internship_locations.csv", index=False)
    industries_table.to_csv(  "/tmp/export_industries.csv",            index=False)
    company_industries_table.to_csv("/tmp/export_company_industries.csv", index=False)


def upload_data():
    to_load = [
        ("/tmp/export_internships.csv",             "TEST2_INTERNSHIPS"),
        ("/tmp/export_companies.csv",               "TEST2_COMPANIES"),
        ("/tmp/export_locations.csv",               "TEST2_LOCATIONS"),
        ("/tmp/export_internship_locations.csv",    "TEST2_INTERNSHIP_LOCATIONS"),
        ("/tmp/export_industries.csv",              "TEST2_INDUSTRIES"),
        ("/tmp/export_company_industries.csv",      "TEST2_COMPANY_INDUSTRIES"),
    ]
    for path, table in to_load:
        df = pd.read_csv(path)
        upload_dataframe_to_snowflake(df, table)
