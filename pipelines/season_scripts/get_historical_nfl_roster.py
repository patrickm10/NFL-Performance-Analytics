import requests
from bs4 import BeautifulSoup
from itertools import product
import polars as pl
from tqdm import tqdm

START_YEAR = 2018
END_YEAR = 2026
historical_years = range(START_YEAR, END_YEAR)

nfl_teams = [
    "arizona-cardinals",
    "atlanta-falcons",
    "baltimore-ravens",
    "buffalo-bills",
    "carolina-panthers",
    "chicago-bears",
    "cincinnati-bengals",
    "cleveland-browns",
    "dallas-cowboys",
    "denver-broncos",
    "detroit-lions",
    "green-bay-packers",
    "houston-texans",
    "indianapolis-colts",
    "jacksonville-jaguars",
    "kansas-city-chiefs",
    "las-vegas-raiders",
    "los-angeles-chargers",
    "los-angeles-rams",
    "miami-dolphins",
    "minnesota-vikings",
    "new-england-patriots",
    "new-orleans-saints",
    "new-york-giants",
    "new-york-jets",
    "philadelphia-eagles",
    "pittsburgh-steelers",
    "san-francisco-49ers",
    "seattle-seahawks",
    "tampa-bay-buccaneers",
    "tennessee-titans",
    "washington-commanders",
]


def get_historical_data(
    session: requests.Session, year: int, team: str
) -> pl.DataFrame | None:
    """
    Return a DataFrame with columns: Player, Year, Team.
    None signals a fetch or parse problem.
    """
    url = f"https://www.nfl.com/sitemap/html/rosters/{year}/{team}"
    print(f"Getting data from {url}")
    try:
        response = session.get(url, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        table = soup.find("table", class_="d3-o-table")
        rows = table.find_all("tr")[1:] if table else []

        if not rows:
            return None

        data = []
        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 2:
                continue
            link = cells[1].find("a")
            player = (
                link.get_text(strip=True) if link else cells[1].get_text(strip=True)
            )
            data.append((player, str(year), team))

        return pl.DataFrame(data, schema=["Player", "Year", "Team"])

    except Exception as exc:
        print(f"Failed {year} {team}: {exc}")
        return None


def main() -> None:
    frames: list[pl.DataFrame] = []
    pairs = product(nfl_teams, historical_years)

    with requests.Session() as session:
        for team, year in tqdm(
            pairs,
            total=len(nfl_teams) * len(historical_years),
            desc="Scraping NFL rosters",
            unit="combo",
        ):
            df = get_historical_data(session, year, team)
            if df is not None and df.height > 0:
                frames.append(df)

    if not frames:
        print("No data collected")
        return

    combined = pl.concat(frames)
    combined.write_csv(
        f"backend/static/data/nfl_metadata/nfl_rosters_{START_YEAR}_{END_YEAR - 1}.csv"
    )
    print(f"Rows written: {combined.height}")
    print(combined)


if __name__ == "__main__":
    main()
