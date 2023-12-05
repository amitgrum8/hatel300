import os
from kafka import KafkaProducer
from src import consts
import json
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
import xgboost as xgb
from catboost import CatBoostRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split


class PreProcessHandler:
    def __init__(self, kafka_server="localhost:9092", kafka_topic="processed_data_one"):
        self.kafka_producer = KafkaProducer(
            bootstrap_servers=kafka_server,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        self.kafka_topic = kafka_topic

    def send_to_kafka(self, data):
        self.kafka_producer.send(self.kafka_topic, data)
        self.kafka_producer.flush()

    def round_data(self, df):
        columns_name = ['dist', 'metro_dist', 'realSum', 'attr_index_norm', 'rest_index_norm']
        for col in columns_name:
            df[col] = df[col].round(5)
        return df

    def create_one_hot_encdoing(self, df):
        binary_columns = ['room_shared', 'room_private', 'host_is_superhost']
        for col in binary_columns:
            df[col] = df[col].map({True: 1, False: 0})
        df['room_type'] = df['room_type'].map({'Private room': 1, 'Entire home/apt': 0})
        return df

    def create_new_table_with_id(self, df):
        df['id'] = df.groupby('dist').cumcount() + 1

    def remove_columns(self, df):
        df['biz'] = (df['multi'] | df['biz'])
        names_of_columns = ["lat", "lng", "attr_index", "rest_index", "multi", "Unnamed: 0"]
        df.drop(columns=names_of_columns, inplace=True)
        df['person_capacity'] = df['person_capacity'].astype(float)
        return df

    def drop_all_nan_and_dup(self, df):
        df.dropna(inplace=True)
        df.drop_duplicates(subset='dist', keep='first', inplace=True)
        return df

    def sort_and_make_new_id(self, df):
        df.sort_values(by='dist', ascending=True, inplace=True)
        df["id"] = range(len(df))
        return df

    def bin_data(self, df):
        columns_name = ["rest_index_norm", "attr_index_norm"]
        for name in columns_name:
            df[name] = pd.cut(df[name], bins=50, labels=False)
        return df

    def pre_process(self, df):
        df = self.round_data(df)
        df = self.create_one_hot_encdoing(df)
        df = self.remove_columns(df)
        df = self.drop_all_nan_and_dup(df)
        df = self.sort_and_make_new_id(df)
        df = self.bin_data(df)
        return df

    def start_pipeline(self):
        files = os.listdir(consts.path_to_dataset)
        for file in files:
            if file != ".ipynb_checkpoints":
                df = pd.read_csv(os.path.join(consts.path_to_dataset, file))
                processed_df = self.pre_process(df)
                self.run_models(processed_df, file)
                file = file.replace(".csv", "")
                json_structure = {
                    "df_data": processed_df.to_dict(orient="split"),  # DataFrame data
                    "property_table_name": f"property_{file}",
                    "renting_table_name": f"renting_{file}"
                }
                self.send_to_kafka(json_structure)  # Send processed data to Kafka

    def run_models(self, df, name):
        if 'realSum' in df.columns:
            X = df.drop(['realSum', "id",
                         "room_type", "room_shared", "room_private", "host_is_superhost", "biz"], axis=1)
            y = df['realSum']
            # column_to_quad = ['attr_index_norm', 'rest_index_norm', "guest_satisfaction_overall"]
            # for name in column_to_quad:
            #     X[name + '_squared'] = X[name] ** 2
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

            models = {
                "XGBoost": xgb.XGBRegressor(),
                "CatBoost": CatBoostRegressor(logging_level='Silent'),
                "randomForest": RandomForestRegressor(n_estimators=100),
                "linear_regression": LinearRegression()
            }
            min_mse = float("inf")
            best_model = None

            for model_name, model in models.items():
                model.fit(X_train, y_train)
                y_pred = model.predict(X_test)
                mse = mean_squared_error(y_test, y_pred)
                print(f"model name {model_name} df:{name} mse:{mse}")
                if mse < min_mse:
                    min_mse = mse
                    best_model = model
            return best_model


p = PreProcessHandler()
p.start_pipeline()
