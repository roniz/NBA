import argparse
import datetime
from enum import Enum
from pathlib import Path
from typing import Iterable, Dict
import pandas as pd
import requests

BEGINNING_OF_NBA_STAT_COLLECTION = 1996

HEADERS = {
    'Host': 'stats.nba.com',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:72.0) Gecko/20100101 Firefox/72.0',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'x-nba-stats-origin': 'stats',
    'x-nba-stats-token': 'true',
    'Connection': 'keep-alive',
    'Referer': 'https://stats.nba.com/',
    'Pragma': 'no-cache',
    'Cache-Control': 'no-cache',
}


class TargetNBAObject(str, Enum):
    Players = 'players'
    Teams = 'teams'


TARGET_NBA_OBJECT_URL_MAP = {
    TargetNBAObject.Players: lambda year:
    f"https://stats.nba.com/stats/leaguedashplayerstats?College=&Conference=&Country=&DateFrom=&DateTo=&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&LastNGames=0&LeagueID=00&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season={year}-{str(year + 1)[-2:]}&SeasonSegment=&SeasonType=Regular%20Season&ShotClockRange=&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight=",
    TargetNBAObject.Teams: lambda year:
    f"https://stats.nba.com/stats/leaguedashteamstats?Conference=&DateFrom=&DateTo=&Division=&GameScope=&GameSegment=&Height=&LastNGames=0&LeagueID=00&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=Totals&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season={year}-{str(year + 1)[-2:]}&SeasonSegment=&SeasonType=Regular%20Season&ShotClockRange=&StarterBench=&TeamID=0&TwoWay=0&VsConference=&VsDivision="
}


def valid_nba_year(year: str) -> int:
    year_as_int = int(year)
    assert year_as_int >= BEGINNING_OF_NBA_STAT_COLLECTION
    assert year_as_int <= datetime.date.today().year
    return year_as_int


def get_stats_from_nba_website(years: Iterable[int], target_nba_object: TargetNBAObject) -> Dict[int, pd.DataFrame]:
    years_stats_dfs_dict = {}
    for year in years:
        year_data = requests.get(TARGET_NBA_OBJECT_URL_MAP[target_nba_object](year),
                                 headers=HEADERS
                                 ).json()
        rows = year_data['resultSets'][0]['rowSet']
        headers = year_data['resultSets'][0]['headers']
        year_df = pd.DataFrame(rows, columns=headers)
        years_stats_dfs_dict[year] = year_df

    return years_stats_dfs_dict


def combine_years_stats(years_stats_dfs_dict: Dict[int, pd.DataFrame]) -> pd.DataFrame:
    seasons_stats_dict = {}
    for year, year_df in years_stats_dfs_dict.items():
        year_df['SEASON'] = year
        seasons_stats_dict[year] = year_df

    all_seasons_df = pd.concat(seasons_stats_dict.values())
    return all_seasons_df


def get_and_format_nba_stats(years: Iterable[int], target_nba_object: TargetNBAObject, output_file_path: Path):
    years_stats_dfs_dict: Dict[int, pd.DataFrame] = get_stats_from_nba_website(years, target_nba_object)
    combined_stats: pd.DataFrame = combine_years_stats(years_stats_dfs_dict)
    combined_stats.to_csv(output_file_path, index=False)


def main():
    parser = argparse.ArgumentParser(
        prog='Get and format NBA data',
        description='',
    )

    parser.add_argument('beginning_year', type=valid_nba_year)
    parser.add_argument('end_year', type=valid_nba_year)
    parser.add_argument('target_nba_object', choices=[target_object.value for target_object in TargetNBAObject])
    parser.add_argument('output_file_path', type=Path)

    args = parser.parse_args()

    years_range = range(args.beginning_year, args.end_year + 1)
    target_nba_object = args.target_nba_object
    output_file_path = args.output_file_path

    get_and_format_nba_stats(years_range,
                             target_nba_object,
                             output_file_path)
    print("Success!")


if __name__ == '__main__':
    main()
