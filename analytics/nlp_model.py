import os
import re
import pandas as pd
from difflib import get_close_matches

# Define stat keywords
STAT_KEYWORDS = {
    "Touchdowns": ["touchdowns", "td", "tds"],
    "YDS": ["passing yards", "pass yards", "py"],
    "Attempts": ["attempts", "att"],
    "Games Played": ["games played", "g"],
    "FPTS": ["fantasy points", "fantasy pts", "pts"],
    "Receptions": ["receptions", "rec"],
    "Rushing Yards": ["rushing yards", "rush yards", "ry"],
    "Rushing Attempts": ["rushing attempts", "rush attempts", "ra"],
    "Y/A": ["yards per attempt", "ypa"],
    "Score": ["score", "scoring"]
}

# Define position keywords and corresponding CSV files
POSITION_KEYWORDS = {
    "QB": ["qb", "quarterback", "quarter back"],
    "RB": ["rb", "runningback", "running back"],
    "WR": ["wr", "wide receiver"],
    "TE": ["te", "tight end"],
    "K": ["k", "kicker"],
    "DEF": ["def", "defense"]
}

def extract_stat_type(question: str):
    """
    Extracts the statistic type from the user's question based on predefined keywords.

    Args:
        question (str): The user's input question.

    Returns:
        str or None: The matching stat type, or None if not found.
    """
    for stat, keywords in STAT_KEYWORDS.items():
        if any(keyword in question.lower() for keyword in keywords):
            return stat
    return None

def extract_position(question: str):
    """
    Extracts the player's position from the user's question based on keywords.

    Args:
        question (str): The user's input question.

    Returns:
        str: The inferred player position, defaults to 'QB' if not found.
    """
    question_lower = question.lower()
    for pos, keywords in POSITION_KEYWORDS.items():
        for keyword in keywords:
            pattern = r'\b' + re.escape(keyword) + r'\b'
            if re.search(pattern, question_lower):
                print(f"[DEBUG] Matched position keyword: '{keyword}' for position {pos}")
                return pos
    return "QB"

def extract_player_name(question: str):
    """
    Cleans and extracts the likely player name from the question by removing
    known keywords and filler words.

    Args:
        question (str): The user's input question.

    Returns:
        str: The extracted and formatted player name.
    """
    question_cleaned = question

    # Remove stat and position keywords
    for kw_list in list(STAT_KEYWORDS.values()) + list(POSITION_KEYWORDS.values()):
        for word in kw_list:
            pattern = r'\b' + re.escape(word) + r'\b'
            question_cleaned = re.sub(pattern, '', question_cleaned, flags=re.IGNORECASE)

    # Remove common filler words
    filler_words = ['what', 'is', 'the', 'of', 'for', 'how many', 'does', 'did', 'in', 'with',
                    'who', 'are', 'a', 'an', 'to', 'this', 'that', 'my', 'your']
    for filler in filler_words:
        pattern = r'\b' + re.escape(filler) + r'\b'
        question_cleaned = re.sub(pattern, '', question_cleaned, flags=re.IGNORECASE)

    # Remove non-alphabetic characters except hyphens and spaces
    question_cleaned = re.sub(r'[^a-zA-Z\s-]', '', question_cleaned)

    # Collapse spaces and title-case
    question_cleaned = re.sub(r'\s+', ' ', question_cleaned).strip()

    return question_cleaned.title()

def load_stats_dataframe(position: str):
    """
    Loads the CSV file containing stats for the given position.

    Args:
        position (str): The position key (e.g., 'QB', 'WR').

    Returns:
        pd.DataFrame or None: The loaded DataFrame or None if file not found or empty.
    """
    file_path = os.path.join("data", "official_rankings", f"official_{position.lower()}_stats.csv")
    if not os.path.exists(file_path):
        print(f"[DEBUG] File not found: {file_path}")
        return None
    df = pd.read_csv(file_path)
    if df.empty:
        print(f"[DEBUG] DataFrame is empty for file: {file_path}")
        return None
    print(f'{df.head()}')  # Debug: Show the first few rows of the DataFrame
    return df

def find_player_row(df, player_name):
    """
    Uses fuzzy matching to find the player's row in the DataFrame.

    Args:
        df (pd.DataFrame): The stats DataFrame.
        player_name (str): The cleaned player name to match.

    Returns:
        pd.DataFrame or None: The matching row as a DataFrame slice, or None if not found.
    """
    df["Player"] = df["Player"].astype(str)
    possible_names = df["Player"].unique().tolist()
    matches = get_close_matches(player_name, possible_names, n=1, cutoff=0.6)
    if not matches:
        print(f"[DEBUG] No fuzzy match for player name: {player_name}")
        return None
    matched_name = matches[0]
    return df[df["Player"] == matched_name]

def answer_question(question: str):
    """
    Processes a user question and returns an appropriate stat answer.

    Args:
        question (str): The input question from the user.

    Returns:
        str: The response containing the stat result or error message.
    """
    stat_type = extract_stat_type(question)
    position = extract_position(question)
    player_name = extract_player_name(question)

    print(f"\n[DEBUG] Player: '{player_name}' | Stat: '{stat_type}' | Position: {position}")

    if not (player_name and stat_type):
        return "Please ask a more complete question, like: 'What are the passing yards of Patrick Mahomes?'"

    df = load_stats_dataframe(position)
    if df is None:
        return f"Could not load stats for position {position}."

    row = find_player_row(df, player_name)
    if row is None or row.empty:
        return f"Could not find {stat_type} stats for {player_name}."

    value = row.iloc[0].get(stat_type)
    if pd.isna(value):
        return f"Stat '{stat_type}' not available for {player_name}."
    
    return f"{player_name} had {value} {stat_type}."

def main():
    """
    Main loop for the CLI tool. Accepts user input and returns stat answers.
    """
    i = 0
    while i > 1:
        question = input("Ask about a player (or type 'q'): ").strip()
        if question.lower() == 'q':
            print("Goodbye!")
            break
        response = answer_question(question)
        print(response)
        i += 1
    print("Exiting the program.")

if __name__ == "__main__":
    main()
