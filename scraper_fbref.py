"""
Scraper for FBref scores & fixtures.

This module defines utility functions to download match fixtures from
FBref for a given league and season range and insert the data into the
`matches` table of the configured MySQL database. It also includes a
top‑level `scrape_all` function that can be invoked from within the
Flask app (via an API route) or executed as a standalone script.

The scraping logic uses `requests` with a rotated User‑Agent and a
Google Referer to avoid common 403 responses. BeautifulSoup is used to
parse the HTML tables. The script gracefully skips seasons or leagues
for which the schedule page returns a non‑200 response. When inserting
into the database, existing rows (based on the unique combination of
league, match_date, home_team and away_team) are ignored to avoid
duplicates.

Note: running this module in an environment without outbound network
access or the ability to install third‑party packages will cause
requests to fail. Deploy this on a service with network access (e.g.
your Render instance) and ensure `requests` and `beautifulsoup4` are
listed in requirements.txt.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Dict, List, Tuple

import mysql.connector
import requests
from bs4 import BeautifulSoup


# Default database configuration. Override these in app.py when
# integrating into Flask. Note: the actual credentials will be
# provided by the environment; these are placeholders.
DB_CONFIG = {
    'host': 'srv1043.hstgr.io',
    'database': 'u827503784_appdevfoot',
    'user': 'u827503784_BnFnX7',
    'password': 'TestApFootProno7+',
    'port': 3306,
    'autocommit': True,
    'pool_size': 3,
    'pool_reset_session': True,
}

# Logging configuration
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Mapping from football-data league codes to FBref competition IDs
# and slugs. These should be kept in sync with the league_mappings
# table. Extend this dictionary as new leagues are added.
LEAGUE_META: Dict[str, Dict[str, str]] = {
    'E0': {'comp_id': '9', 'slug': 'Premier-League'},
    'SP1': {'comp_id': '12', 'slug': 'La-Liga'},
    'F1': {'comp_id': '13', 'slug': 'Ligue-1'},
    'D1': {'comp_id': '20', 'slug': 'Bundesliga'},
    'I1': {'comp_id': '11', 'slug': 'Serie-A'},
}

# Helper to build list of seasons between start and end
def generate_seasons(start: str, end: str) -> List[str]:
    """Generate a list of season strings from start to end inclusive.

    Seasons are of the form "YYYY-YYYY". The start season should be
    earlier or equal to the end season. If parsing fails, an empty
    list is returned.
    """
    try:
        start_year = int(start.split('-')[0])
        end_year = int(end.split('-')[0])
    except Exception:
        logger.error("Invalid season format: %s, %s", start, end)
        return []
    seasons: List[str] = []
    for year in range(start_year, end_year + 1):
        seasons.append(f"{year}-{year + 1}")
    return seasons


def fetch_schedule_html(comp_id: str, slug: str, season: str, retries: int = 3) -> str | None:
    """Fetch the schedule HTML for a given competition and season.

    Args:
        comp_id: FBref competition ID.
        slug: FBref slug for the league.
        season: Season string (e.g., "2022-2023").
        retries: Number of times to retry on non‑200 status.

    Returns:
        HTML content of the schedule page, or None if retrieval fails.
    """
    url = f"https://fbref.com/en/comps/{comp_id}/{season}/schedule/{season}-{slug}-Scores-and-Fixtures"
    headers_list = [
        {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/117.0',
            'Referer': 'https://www.google.com/'
        },
        {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
            'Referer': 'https://www.google.com/'
        },
    ]
    for attempt in range(retries):
        headers = headers_list[attempt % len(headers_list)]
        try:
            resp = requests.get(url, headers=headers, timeout=30)
        except Exception as e:
            logger.warning("Error fetching %s: %s", url, e)
            time.sleep(1.5 ** attempt)
            continue
        if resp.status_code == 200:
            return resp.text
        elif resp.status_code == 404:
            # No schedule available for this season
            logger.info("Schedule not found (404) for %s %s", slug, season)
            return None
        else:
            logger.warning("Unexpected status %s for %s", resp.status_code, url)
            time.sleep(1.5 ** attempt)
    logger.error("Failed to fetch schedule for %s %s after %d retries", slug, season, retries)
    return None


def parse_schedule(html: str, league_code: str, season: str) -> List[Dict[str, str]]:
    """Parse the schedule HTML and extract matches.

    Each match is represented as a dictionary with keys: league, season,
    match_date (ISO format), home_team, away_team, home_score, away_score,
    home_xg, away_xg. If xG values are unavailable they are set to None.

    Args:
        html: The raw HTML of the schedule page.
        league_code: Football data league code (e.g., "E0").
        season: Season string.

    Returns:
        A list of match dictionaries.
    """
    soup = BeautifulSoup(html, 'html.parser')
    # Locate the table that contains the schedule; FBref uses a table with
    # summary starting with "Scores & Fixtures".
    table = soup.find('table', summary=lambda s: s and 'Scores & Fixtures' in s)
    if not table:
        logger.warning("Could not find schedule table for %s %s", league_code, season)
        return []
    matches: List[Dict[str, str]] = []
    rows = table.find_all('tr')[1:]
    for row in rows:
        cols = [c.get_text(strip=True) for c in row.find_all(['th', 'td'])]
        # Rows may have varying number of columns if postponed/cancelled.
        # Expected columns order: Wk, Day, Date, Time, Home, xG_home, Score,
        # xG_away, Away, Attendance, Venue, Referee, Match Report, Notes.
        # We only need date, home, away, score and xG.
        if len(cols) < 8:
            # Skip incomplete rows (headers, separators)
            continue
        date_str = cols[2]
        # Some dates may be blank for rescheduled matches; skip them.
        if not date_str:
            continue
        try:
            match_date = datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            # Date may include weekday abbreviations; attempt alternate parse
            try:
                match_date = datetime.strptime(date_str.split(' ')[0], '%Y-%m-%d')
            except Exception:
                logger.debug("Unparseable date '%s' for %s %s", date_str, league_code, season)
                continue
        home_team = cols[4]
        # Score column typically like "2–1" with en dash; unify to hyphen
        score_raw = cols[6].replace('–', '-').replace('—', '-')
        if score_raw == '' or score_raw.lower() == 'postponed':
            # Skip postponed/cancelled
            continue
        try:
            home_score_str, away_score_str = score_raw.split('-')
        except ValueError:
            # Unexpected format; skip
            logger.debug("Unexpected score format '%s' for %s %s", score_raw, league_code, season)
            continue
        try:
            home_score = int(home_score_str)
            away_score = int(away_score_str)
        except ValueError:
            continue
        away_team = cols[8]
        # xG values may be missing; attempt to parse
        home_xg = None
        away_xg = None
        try:
            home_xg = float(cols[5]) if cols[5] else None
        except ValueError:
            pass
        try:
            away_xg = float(cols[7]) if cols[7] else None
        except ValueError:
            pass
        matches.append({
            'league': league_code,
            'season': season,
            'match_date': match_date.strftime('%Y-%m-%d'),
            'home_team': home_team,
            'away_team': away_team,
            'home_score': home_score,
            'away_score': away_score,
            'home_xg': home_xg,
            'away_xg': away_xg,
        })
    return matches


def insert_matches_to_db(matches: List[Dict[str, str]], db_config: Dict[str, any]) -> int:
    """Insert a list of matches into the `matches` table.

    Uses INSERT IGNORE to avoid inserting duplicates based on the
    unique index on (league, match_date, home_team, away_team).

    Args:
        matches: List of match dictionaries.
        db_config: MySQL connection parameters.

    Returns:
        The number of rows inserted (ignored rows are not counted).
    """
    if not matches:
        return 0
    inserted = 0
    conn = None
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        insert_sql = (
            "INSERT IGNORE INTO matches "
            "(league, season, match_date, home_team, away_team, home_score, away_score, home_xg, away_xg, created_at, updated_at) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),NOW())"
        )
        data = [
            (
                m['league'], m['season'], m['match_date'], m['home_team'], m['away_team'],
                m['home_score'], m['away_score'], m['home_xg'], m['away_xg']
            ) for m in matches
        ]
        cursor.executemany(insert_sql, data)
        inserted = cursor.rowcount
        conn.commit()
        cursor.close()
    except Exception as e:
        logger.error("Error inserting matches: %s", e)
    finally:
        if conn:
            conn.close()
    return inserted


def scrape_league(league_code: str, start: str, end: str, db_config: Dict[str, any]) -> int:
    """Scrape all seasons for a specific league and insert into DB.

    Args:
        league_code: Football data league code.
        start: Start season (e.g., "2000-2001").
        end: End season (e.g., "2024-2025").
        db_config: Database connection parameters.

    Returns:
        Total number of matches inserted for this league.
    """
    meta = LEAGUE_META.get(league_code)
    if not meta:
        logger.error("Unknown league code: %s", league_code)
        return 0
    comp_id = meta['comp_id']
    slug = meta['slug']
    seasons = generate_seasons(start, end)
    total_inserted = 0
    for season in seasons:
        html = fetch_schedule_html(comp_id, slug, season)
        if not html:
            continue
        matches = parse_schedule(html, league_code, season)
        if not matches:
            continue
        inserted = insert_matches_to_db(matches, db_config)
        total_inserted += inserted
        logger.info("%s %s: inserted %d new matches", league_code, season, inserted)
        # polite pause to avoid overloading the server
        time.sleep(0.5)
    return total_inserted


def scrape_all(leagues: List[str] | None = None, start: str = '2000-2001', end: str = '2024-2025', db_config: Dict[str, any] | None = None) -> Dict[str, int]:
    """Scrape multiple leagues and seasons.

    Args:
        leagues: List of league codes to scrape. If None, scrape all in LEAGUE_META.
        start: Start season.
        end: End season.
        db_config: Optional DB configuration override.

    Returns:
        Dictionary mapping league codes to number of matches inserted.
    """
    if db_config is None:
        db_config = DB_CONFIG
    if leagues is None:
        leagues = list(LEAGUE_META.keys())
    results: Dict[str, int] = {}
    for league_code in leagues:
        inserted = scrape_league(league_code, start, end, db_config)
        results[league_code] = inserted
    return results


if __name__ == '__main__':
    # Standalone execution for manual runs
    logging.basicConfig(level=logging.INFO)
    leagues = list(LEAGUE_META.keys())
    results = scrape_all(leagues, '2000-2001', '2024-2025')
    print("Scraping complete:", results)