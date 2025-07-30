import duckdb
import polars as pl
from pathlib import Path

DATA_PATH = "backend/static/data/official_rankings/historical/qb_week_rankings_2020_2025.csv"

# Weather condition thresholds
RAIN_MM_LIGHT = 0.5
WIND_KPH_WINDY = 25.0
COLD_C = 5.0
FREEZING_C = 0.0

MIN_GAMES = 3

def setup_duckdb_connection(csv_path: str):
    con = duckdb.connect()
    con.execute(f"""
        CREATE OR REPLACE VIEW qb_data AS
        SELECT

            TRIM(Player) AS Player_clean,

            CAST(CMP AS DOUBLE)           AS CMP,
            CAST("Pass_Att" AS DOUBLE)    AS Pass_Att,
            CAST("Pass_Yds" AS DOUBLE)    AS Pass_Yds,
            CAST("Pass_TD" AS DOUBLE)     AS Pass_TD,
            CAST(INT AS DOUBLE)           AS INT,
            CAST(FPTS AS DOUBLE)          AS FPTS,

            CAST(elevation AS DOUBLE)     AS elevation,
            CAST(temp_C AS DOUBLE)        AS temp_C,
            CAST(precip_mm AS DOUBLE)     AS precip_mm,
            CAST(wind_kph AS DOUBLE)      AS wind_kph,
            CAST(rel_humidity AS DOUBLE)  AS rel_humidity,
            CAST(pressure_hpa AS DOUBLE)  AS pressure_hpa,

            CAST(Year AS INTEGER)         AS year,
            CAST(Week AS INTEGER)         AS week,

            COALESCE(indoor_outdoor, '')  AS indoor_outdoor,
            COALESCE(surface_type, '')    AS surface_type,

            CASE WHEN precip_mm >= {RAIN_MM_LIGHT}   THEN TRUE ELSE FALSE END AS is_rain_game,
            CASE WHEN wind_kph  >= {WIND_KPH_WINDY}  THEN TRUE ELSE FALSE END AS is_windy_game,
            CASE WHEN temp_C    <= {COLD_C}          THEN TRUE ELSE FALSE END AS is_cold_game,
            CASE
                WHEN (precip_mm >= {RAIN_MM_LIGHT})
                  OR (wind_kph >= {WIND_KPH_WINDY})
                  OR (temp_C <= {COLD_C})
                THEN TRUE ELSE FALSE
            END AS is_messy_game
        FROM read_csv_auto('{csv_path}', header=True, ignore_errors=True)
    """)

    # View restricted to most recent season
    con.execute("""
        CREATE OR REPLACE VIEW qb_season AS
        SELECT *
        FROM qb_data
        WHERE year = (SELECT max(year) FROM qb_data)
    """)
    return con

def season_year(con) -> int:
    return con.execute("SELECT max(year) FROM qb_data").fetchone()[0]

def fetch_pl_df(con, query: str) -> pl.DataFrame:
    return pl.from_pandas(con.execute(query).fetchdf())

def best_qbs_overall(con):
    q = f"""
        SELECT
            Player_clean AS Player,
            ROUND(AVG(FPTS), 2)    AS avg_fantasy_points,
            ROUND(AVG(Pass_TD), 2) AS avg_pass_tds,
            ROUND(AVG(Pass_Yds), 1)AS avg_pass_yds,
            COUNT(DISTINCT CAST(year AS VARCHAR) || '-' || CAST(week AS VARCHAR)) AS games_played
        FROM qb_season
        GROUP BY Player
        HAVING games_played >= {MIN_GAMES}
        ORDER BY avg_fantasy_points DESC
        LIMIT 50
    """
    return fetch_pl_df(con, q)

def indoor_vs_outdoor(con):
    q = f"""
        SELECT
            Player_clean AS Player,
            indoor_outdoor,
            ROUND(AVG(FPTS), 2) AS avg_fantasy_points,
            COUNT(*) AS games
        FROM qb_season
        WHERE indoor_outdoor IN ('Indoor','Outdoor')
        GROUP BY Player, indoor_outdoor
        HAVING COUNT(*) >= {MIN_GAMES}
        ORDER BY avg_fantasy_points DESC
        LIMIT 25
    """
    return fetch_pl_df(con, q)

def surface_type_impact(con):
    q = f"""
        SELECT
            Player_clean AS Player,
            surface_type,
            ROUND(AVG(FPTS), 2) AS avg_fantasy_points,
            COUNT(*) AS games
        FROM qb_season
        WHERE surface_type IN ('Grass','Turf')
        GROUP BY Player, surface_type
        HAVING COUNT(*) >= {MIN_GAMES}
        ORDER BY avg_fantasy_points DESC
        LIMIT 25
    """
    return fetch_pl_df(con, q)

def elevation_impact(con):
    q = f"""
        SELECT
            Player_clean AS Player,
            CASE
                WHEN elevation >= 500 THEN 'High'
                WHEN elevation BETWEEN 100 AND 499 THEN 'Medium'
                ELSE 'Low'
            END AS elevation_level,
            ROUND(AVG(FPTS), 2) AS avg_fantasy_points,
            COUNT(*) AS games
        FROM qb_season
        GROUP BY Player, elevation_level
        HAVING COUNT(*) >= {MIN_GAMES}
        ORDER BY avg_fantasy_points DESC
        LIMIT 25
    """
    return fetch_pl_df(con, q)

def rain_game_performance(con):
    q = f"""
        SELECT
            Player_clean AS Player,
            CASE WHEN precip_mm >= {RAIN_MM_LIGHT} THEN 'Rain' ELSE 'No Rain' END AS rain_category,
            ROUND(AVG(FPTS), 2)    AS avg_fantasy_points,
            ROUND(AVG(Pass_Yds),1) AS avg_pass_yds,
            ROUND(AVG(Pass_TD), 2) AS avg_pass_tds,
            COUNT(*) AS games
        FROM qb_season
        WHERE precip_mm IS NOT NULL
        GROUP BY Player, rain_category
        HAVING COUNT(*) >= {MIN_GAMES}
        ORDER BY rain_category, avg_fantasy_points DESC
        LIMIT 25
    """
    return fetch_pl_df(con, q)

def windy_game_performance(con):
    q = f"""
        SELECT
            Player_clean AS Player,
            CASE WHEN wind_kph >= {WIND_KPH_WINDY} THEN 'Windy' ELSE 'Calm' END AS wind_category,
            ROUND(AVG(FPTS), 2)    AS avg_fantasy_points,
            ROUND(AVG(Pass_Yds),1) AS avg_pass_yds,
            ROUND(AVG(Pass_TD), 2) AS avg_pass_tds,
            COUNT(*) AS games
        FROM qb_season
        WHERE wind_kph IS NOT NULL
        GROUP BY Player, wind_category
        HAVING COUNT(*) >= {MIN_GAMES}
        ORDER BY wind_category, avg_fantasy_points DESC
        LIMIT 25
    """
    return fetch_pl_df(con, q)

def temp_band_performance(con):
    q = f"""
        WITH banded AS (
            SELECT
                Player_clean AS Player,
                CASE
                    WHEN temp_C <= {FREEZING_C} THEN 'Freezing'
                    WHEN temp_C <= {COLD_C}    THEN 'Cold'
                    WHEN temp_C <= 15          THEN 'Cool'
                    WHEN temp_C <= 25          THEN 'Mild'
                    ELSE 'Warm'
                END AS temp_band,
                FPTS, Pass_Yds, Pass_TD
            FROM qb_season
            WHERE temp_C IS NOT NULL
        )
        SELECT
            Player,
            temp_band,
            ROUND(AVG(FPTS), 2)    AS avg_fantasy_points,
            ROUND(AVG(Pass_Yds),1) AS avg_pass_yds,
            ROUND(AVG(Pass_TD), 2) AS avg_pass_tds,
            COUNT(*) AS games
        FROM banded
        GROUP BY Player, temp_band
        HAVING COUNT(*) >= {MIN_GAMES}
        ORDER BY temp_band, avg_fantasy_points DESC
        LIMIT 50
    """
    return fetch_pl_df(con, q)

def messy_weather_performance(con):
    q = f"""
        SELECT
            Player_clean AS Player,
            CASE WHEN is_messy_game THEN 'Messy' ELSE 'Normal' END AS weather_class,
            ROUND(AVG(FPTS), 2)    AS avg_fantasy_points,
            ROUND(AVG(Pass_Yds),1) AS avg_pass_yds,
            ROUND(AVG(Pass_TD), 2) AS avg_pass_tds,
            COUNT(*) AS games
        FROM qb_season
        GROUP BY Player, weather_class
        HAVING COUNT(*) >= {MIN_GAMES}
        ORDER BY weather_class, avg_fantasy_points DESC
        LIMIT 25
    """
    return fetch_pl_df(con, q)

def top_qbs_in_messy(con):
    q = f"""
        SELECT
            Player_clean AS Player,
            ROUND(AVG(FPTS), 2)    AS avg_fantasy_points_messy,
            ROUND(AVG(Pass_Yds),1) AS avg_pass_yds_messy,
            ROUND(AVG(Pass_TD), 2) AS avg_pass_tds_messy,
            COUNT(*) AS messy_games
        FROM qb_season
        WHERE is_messy_game
        GROUP BY Player
        HAVING messy_games >= {MIN_GAMES}
        ORDER BY avg_fantasy_points_messy DESC
        LIMIT 25
    """
    return fetch_pl_df(con, q)

def weather_correlations(con):
    q_all = """
        SELECT
            corr(FPTS, precip_mm)    AS corr_fpts_precip,
            corr(FPTS, wind_kph)     AS corr_fpts_wind,
            corr(FPTS, temp_C)       AS corr_fpts_temp,
            corr(FPTS, rel_humidity) AS corr_fpts_humidity,
            corr(FPTS, pressure_hpa) AS corr_fpts_pressure
        FROM qb_season
        WHERE precip_mm IS NOT NULL
           OR wind_kph  IS NOT NULL
           OR temp_C    IS NOT NULL
    """
    q_outdoor = """
        SELECT
            corr(FPTS, precip_mm) AS corr_fpts_precip_outdoor,
            corr(FPTS, wind_kph)  AS corr_fpts_wind_outdoor,
            corr(FPTS, temp_C)    AS corr_fpts_temp_outdoor
        FROM qb_season
        WHERE indoor_outdoor = 'Outdoor'
          AND (precip_mm IS NOT NULL OR wind_kph IS NOT NULL OR temp_C IS NOT NULL)
    """
    return fetch_pl_df(con, q_all), fetch_pl_df(con, q_outdoor)

def main():
    if not Path(DATA_PATH).exists():
        raise FileNotFoundError(f"CSV not found at {DATA_PATH}")

    con = setup_duckdb_connection(DATA_PATH)
    yr = season_year(con)
    print(f"Season year in use: {yr}")

    print("\n=== Best QBs Overall ===")
    print(best_qbs_overall(con))

    print("\n=== Indoor vs Outdoor ===")
    print(indoor_vs_outdoor(con))

    print("\n=== Surface Type Impact ===")
    print(surface_type_impact(con))

    print("\n=== Elevation Impact ===")
    print(elevation_impact(con))

    print("\n=== Rain vs No Rain ===")
    print(rain_game_performance(con))

    print("\n=== Windy vs Calm ===")
    print(windy_game_performance(con))

    print("\n=== Temperature Bands ===")
    print(temp_band_performance(con))

    print("\n=== Messy vs Normal ===")
    print(messy_weather_performance(con))

    print("\n=== Top QBs in Messy Weather ===")
    print(top_qbs_in_messy(con))

    print("\n=== Correlations ===")
    df_all, df_outdoor = weather_correlations(con)
    print(df_all)
    print(df_outdoor)

    con.close()

if __name__ == "__main__":
    main()
