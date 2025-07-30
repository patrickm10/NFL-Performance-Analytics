import duckdb
import polars as pl


def clean_name_expr(column: str) -> str:
    return f"LOWER(TRIM({column}))"

def enrich_historical_sql(position: str):
    con = duckdb.connect()

    historical_path = f"data/official_rankings/historical/official_{position}_2020_2025_historical_data.csv"

    # Materialize historical subset
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE historical_tbl AS
        SELECT *, {clean_name_expr('Player')} AS player_key
        FROM read_csv_auto('{historical_path}', header=True);
    """)

    # Materialize roster and stadiums with cleaned keys
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE roster_tbl AS
        SELECT *, {clean_name_expr('Player')} AS player_key
        FROM read_csv_auto('data/nfl_metadata/nfl_roster.csv', header=True);
    """)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE stadiums_tbl AS
        SELECT *, {clean_name_expr('home_team_name')} AS home_team_key
        FROM read_csv_auto('data/nfl_metadata/nfl_stadiums.csv', header=True);
    """)

    # Enrich historical with roster and stadium info
    result = con.execute(f"""
        SELECT 
            h.week, h.year, h.Player, h.CMP, h.ATT, h.YDS, h.TD, h.INT, h.FPTS,
            COALESCE(r.home_team_name, h.home_team_name) AS home_team_name,
            s.stadium_name, s.indoor_outdoor, s.surface_type,
            s.weather_impact, s.elevation, s.year_opened
        FROM historical_tbl h
        LEFT JOIN roster_tbl r ON h.player_key = r.player_key
        LEFT JOIN stadiums_tbl s ON {clean_name_expr('COALESCE(r.home_team_name, h.home_team_name)')} = s.home_team_key
        ORDER BY h.year, h.week;
    """).fetchdf()

    missing = result[result['stadium_name'].isnull()]
    print(f"\nRows missing stadium metadata: {len(missing)}")
    print(missing[['week', 'year', 'Player', 'home_team_name']].reset_index(drop=True))

    output_path = f"data/official_rankings/historical/official_{position}_2020_2025_historical_data.csv"
    result.to_csv(output_path, index=False)
    print(f"Enriched stadium metadata saved to: {output_path}")

    # --- Now enrich total_nfl_matchups_with_stadiums.csv with missing stadium info ---
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE matchups_with_missing AS
        SELECT *
        FROM read_csv_auto('data/nfl_metadata/total_nfl_matchups_with_stadiums.csv', header=True)
        WHERE stadium_name IS NULL OR stadium_name = '';
    """)

    # Update missing stadiums by joining on home_team_name
    enriched_matchups = con.execute(f"""
        SELECT 
            m.week, m.year, m.home_team_name, m.away_team_name,
            COALESCE(s.stadium_name, m.stadium_name) AS stadium_name,
            COALESCE(s.indoor_outdoor, m.indoor_outdoor) AS indoor_outdoor,
            COALESCE(s.surface_type, m.surface_type) AS surface_type,
            COALESCE(s.weather_impact, m.weather_impact) AS weather_impact,
            COALESCE(s.elevation, m.elevation) AS elevation,
            COALESCE(s.year_opened, m.year_opened) AS year_opened
        FROM matchups_with_missing m
        LEFT JOIN stadiums_tbl s ON {clean_name_expr('m.home_team_name')} = s.home_team_key
    """).fetchdf()

    # Reload all matchups to merge updated missing records back
    all_matchups = con.execute("SELECT * FROM read_csv_auto('data/nfl_metadata/total_nfl_matchups_with_stadiums.csv', header=True)").fetchdf()

    all_pl = pl.from_pandas(all_matchups)
    enriched_pl = pl.from_pandas(enriched_matchups)

    # Filter out the missing records from all_pl and then append enriched ones
    filtered_all = all_pl.filter(~all_pl.lazy().filter(
        (pl.col("stadium_name").is_null()) | (pl.col("stadium_name") == "")
    ).collect().to_series())

    updated_matchups = filtered_all.vstack(enriched_pl)

    # Save updated matchups file
    updated_matchups.write_csv('data/nfl_metadata/total_nfl_matchups_with_stadiums.csv')
    print("Updated total_nfl_matchups_with_stadiums.csv with missing stadium metadata.")

def main():
    for pos in ["qb"]:
        enrich_historical_sql(pos)

if __name__ == "__main__":
    main()
