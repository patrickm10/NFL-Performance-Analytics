import pandas as pd
import logging
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def prepare_seasonal_data(df):
    df = df.sort_values(by=["Player", "YEAR"])
    df["Prev_YDS"] = df.groupby("Player")["YDS"].shift(1)
    df["Prev_TD"] = df.groupby("Player")["TD"].shift(1)
    df["Prev_INT"] = df.groupby("Player")["INT"].shift(1)
    df["Prev_COMP"] = df.groupby("Player")["COMP"].shift(1)
    df["Prev_ATT"] = df.groupby("Player")["ATT"].shift(1)
    return df.dropna()

def build_pipeline(feature_columns):
    preprocessor = ColumnTransformer([
        ("scaler", StandardScaler(), feature_columns)
    ])
    pipeline = Pipeline([
        ("preprocessor", preprocessor),
        ("regressor", RandomForestRegressor(n_estimators=100, random_state=42))
    ])
    return pipeline

def predict_2025(file_path, target_column, prediction_label):
    df = pd.read_csv(file_path)
    if df is None or df.empty:
        logging.error("Failed to load data or dataset is empty")
        return
    required_columns = {"Player", "YEAR", "TD", "YDS", "INT", "COMP", "ATT"}
    if not required_columns.issubset(df.columns):
        logging.error(f"Missing required columns: {required_columns - set(df.columns)}")
        return
    df = prepare_seasonal_data(df)
    if df.empty:
        logging.error("Insufficient data after feature engineering.")
        return
    train_df = df[df["YEAR"] < 2025]
    predict_df = df[df["YEAR"] == 2024]
    feature_columns = ["Prev_YDS", "Prev_TD", "Prev_INT", "Prev_COMP", "Prev_ATT"]
    X_train = train_df[feature_columns]
    y_train = train_df[target_column]
    X_predict = predict_df[feature_columns]
    player_info = predict_df[["Player", "YEAR", "TD", "YDS", "INT", "COMP", "ATT"]]
    pipeline = build_pipeline(feature_columns)
    pipeline.fit(X_train, y_train)
    y_pred_2025 = pipeline.predict(X_predict)
    predictions = player_info.copy()
    predictions[prediction_label] = y_pred_2025
    logging.info(f"\nPredicted 2025 {prediction_label.replace('_', ' ')}:")
    logging.info(predictions.to_string(index=False))
    return pipeline, predictions

def predict_2025_yards(file_path):
    return predict_2025(file_path, target_column="YDS", prediction_label="Predicted_YDS_2025")

def predict_2025_td(file_path):
    return predict_2025(file_path, target_column="TD", prediction_label="Predicted_TD_2025")

predict_2025_yards("qb_stats/qb_career_stats/Baker_Mayfield_career_passing_stats.csv")
predict_2025_td("qb_stats/qb_career_stats/Baker_Mayfield_career_passing_stats.csv")
