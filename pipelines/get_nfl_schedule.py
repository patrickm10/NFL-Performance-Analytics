from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import warnings
import polars as pl

warnings.filterwarnings("ignore")

def build_driver():
    options = Options()
    options.headless = True
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1920,1080")
    return webdriver.Chrome(options=options)

def scrape_game_location(driver, url, cache):
    if url in cache:
        return cache[url]

    driver.get(url)
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div[class*='r-color-zyhucb']"))
        )
        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")
        venue_div = soup.select_one("div[class*='r-color-zyhucb']")
        location = venue_div.text.strip() if venue_div else None
    except Exception as e:
        print(f"Failed to load game page {url}: {e}")
        location = None

    cache[url] = location
    return location

def scrape_week(driver, year, week, cache):
    url = f"https://www.nfl.com/schedules/{year}/REG{week}/"
    driver.get(url)

    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CLASS_NAME, "nfl-c-matchup-strip__left-area"))
        )
        soup = BeautifulSoup(driver.page_source, "html.parser")
    except Exception as e:
        print(f"Week {week} failed to load: {e}")
        return []

    games = []
    for idx, link in enumerate(soup.select("a.nfl-c-matchup-strip__left-area"), 1):
        game_div = link.select_one("div.nfl-c-matchup-strip__game")
        if not game_div:
            continue

        teams = []
        team_divs = game_div.select("div.nfl-c-matchup-strip__team")
        record_divs = game_div.select("div.css-12hprx4-U7")

        for i, td in enumerate(team_divs):
            abbr = td.select_one("span.nfl-c-matchup-strip__team-abbreviation")
            name = td.select_one("span.nfl-c-matchup-strip__team-fullname")
            teams.append({
                "abbreviation": abbr.text.strip() if abbr else None,
                "fullname": name.text.strip() if name else None,
                "record": record_divs[i].text.strip() if i < len(record_divs) else None
            })

        date = link.select_one("span.nfl-c-matchup-strip__date-time")
        tz = link.select_one("span.nfl-c-matchup-strip__date-timezone")
        time = f"{date.text.strip()} {tz.text.strip()}" if date and tz else None

        game_url = f"https://www.nfl.com{link.get('href')}"
        location = scrape_game_location(driver, game_url, cache)

        for team in teams:
            games.append({
                "week": week,
                "game_number": idx,
                "team_abbreviation": team["abbreviation"],
                "team_fullname": team["fullname"],
                "team_record": team["record"],
                "time": time,
                "location": location
            })
    print(games)

    return games

def main(year=2025):
    driver = build_driver()
    location_cache = {}
    all_games = []

    for week in range(1, 19):
        print(f"Scraping week {week}")
        all_games.extend(scrape_week(driver, year, week, location_cache))

    driver.quit()
    df = pl.DataFrame(all_games)
    df.write_csv(f"nfl_schedule_{year}.csv")
    print(df)
    print(f"Saved to nfl_schedule_{year}.csv")

if __name__ == "__main__":
    main()
