import re
import numpy as np
import pandas as pd

# ——————— helper at module scope ———————
degree_pattern = re.compile(r'^(Undergrad|Masters|PhD)\b', flags=re.IGNORECASE)

def split_perks(cell):
    """
    Splits out a leading degree requirement (Undergrad/Masters/PhD)
    from the perks text. Returns a Series with two fields.
    """
    if pd.isna(cell):
        return pd.Series({'degree_requirement': np.nan, 'perks_clean': np.nan})

    # normalize quotes & whitespace
    text = str(cell).strip().strip('"')

    # handle both real newlines and literal "\n"
    parts = re.split(r'(?:\\n|\n)', text, maxsplit=1)

    # fixing degrees and perks issue
    if len(parts) > 1 and degree_pattern.match(parts[0]):
        return pd.Series({
            'degree_requirement': parts[0].strip(),
            'perks_clean':        parts[1].strip()
        })

    # accounting for if there's only degree reqs and no perks
    if degree_pattern.match(text) and len(parts) == 1:
        return pd.Series({
            'degree_requirement': text.strip(),
            'perks_clean':        ''
        })

    # if there aren't degree reqs at all
    return pd.Series({
        'degree_requirement': np.nan,
        'perks_clean':        text
    })

def clean_internships(df_interns_raw):
    df = df_interns_raw.copy()

    # casting to nums
    df['hourly_rate'] = pd.to_numeric(df['hourly_rate'], errors='coerce')
    df['monthly_pay'] = pd.to_numeric(df['monthly_pay'], errors='coerce')

    # slug & name cleanup
    df['company_slug'] = df['company_slug'].str.lower().str.strip()
    df['company_name'] = df['company_name'].str.strip()

    df[['degree_requirement','perks_clean']] = df['perks'].apply(split_perks)

    return df


def clean_companies(df_companies_raw):
    df = df_companies_raw.copy()

    df['year_founded']  = pd.to_numeric(df['year_founded'],  errors='coerce')
    df['num_employees'] = pd.to_numeric(df['num_employees'], errors='coerce')
    df['company_slug']  = df['company_slug'].str.lower().str.strip()

    return df


def clean_simplify_profiles(df_simplify_raw):
    df = df_simplify_raw.copy()

    df['company_name'] = df['company_name'].str.strip()

    return df

# building extra tables to fulfill 3NF

def build_locations_table(df_interns):
    locs = set(zip(df_interns['city'], df_interns['state'], df_interns['country']))
    return pd.DataFrame(locs, columns=['city', 'state', 'country'])


def build_internship_locations_table(df_interns):
    rows = [
        (row.internship_id, row.city, row.state, row.country)
        for _, row in df_interns.iterrows()
    ]
    return pd.DataFrame(rows, columns=['internship_id', 'city', 'state', 'country'])


def build_industries_table(df_interns):
    industry_set = set()
    for inds in df_interns['industries'].dropna():
        for ind in inds.split(','):
            industry_set.add(ind.strip())
    return pd.DataFrame({'name': sorted(industry_set)})


def build_company_industries_table(df_interns):
    pairs = []
    for _, row in df_interns.iterrows():
        if pd.notna(row['industries']):
            for ind in row['industries'].split(','):
                ind = ind.strip()
                if ind:
                    pairs.append((row['normalized_slug'], ind))
    return pd.DataFrame(pairs, columns=['normalized_slug', 'industry_name'])
