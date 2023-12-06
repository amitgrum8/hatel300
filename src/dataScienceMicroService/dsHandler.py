from kafka import KafkaConsumer
import json
import logging
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
import xgboost as xgb
from catboost import CatBoostRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split
from sklearn.neighbors import KNeighborsRegressor
from sqlalchemy import Column, Integer, String, ARRAY, create_engine, Float
from sqlalchemy.orm import sessionmaker, declarative_base
import matplotlib.pyplot as plt
from src import consts

Base = declarative_base()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class LinearRegressionModel(Base):
    __tablename__ = 'linear_regression_models'
    id = Column(Integer, primary_key=True)
    table_name = Column(String, unique=True)
    slope = Column(ARRAY(Float))  # Assuming slope is an array
    intercept = Column(Float)


class DsHandler:
    def __init__(self, topic=consts.DS_DATA_TOPIC, bootstrap_servers='localhost:9092'):
        self.topic = topic
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='ds-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=30000
        )
        self.tables_data = {}
        self.engine = None

    def initialize_database(self):
        db_config = consts.db_config
        self.engine = create_engine(
            f'postgresql://{db_config["user"]}:{db_config["password"]}@{db_config["host"]}:{db_config["port"]}/{db_config["database"]}'
        )
        Base.metadata.create_all(self.engine)

    def consume_data(self):
        logger.info(f"Starting to consume from topic: {self.topic}")
        try:
            for message in self.consumer:
                try:
                    data = json.loads(message.value)
                except json.JSONDecodeError as e:
                    logger.error(f"Error decoding JSON data from Kafka message: {e}")
                    continue
                if isinstance(data, dict) and 'table_name' in data and 'data' in data:
                    table_name = data['table_name']
                    if table_name not in ["linear_regression_models", "main_table"]:
                        df_data = json.loads(data['data'])
                        df = pd.DataFrame(df_data)
                        self.tables_data[table_name] = df
                else:
                    logger.error("Invalid data format received from Kafka.")
        except StopIteration:
            logger.info("No new messages in the last 30 seconds, stopping the consumer...")
        finally:
            self.consumer.close()
            self.start_prediction()

    def start_prediction(self):
        self.merge_tables()
        for table_name, df in self.tables_data.items():
            if 'realSum' in df.columns:
                print(table_name)
                best_model, model_mse = self.perform_model_evaluation(df, table_name)
                self.plot(model_mse)

    def plot(self, model_mse):
        plt.figure(figsize=(10, 6))
        plt.bar(model_mse.keys(), model_mse.values(), color='skyblue')
        plt.xlabel('Model')
        plt.ylabel('Mean Squared Error')
        plt.title('Model Comparison based on MSE')
        plt.xticks(rotation=45)
        plt.show()

    def save_model_parameters(self, table_name, slope, intercept):
        Session = sessionmaker(bind=self.engine)
        session = Session()
        model = LinearRegressionModel(table_name=table_name, slope=slope, intercept=intercept)
        session.add(model)
        session.commit()
        session.close()

    def get_base_name(self, table_name):
        if 'property_' in table_name:
            return table_name.replace('property_', '')
        elif 'renting_' in table_name:
            return table_name.replace('renting_', '')
        return table_name

    def merge_tables(self):
        merged_tables = {}
        for table_name in list(self.tables_data.keys()):
            base_name = self.get_base_name(table_name)
            property_table_name = f'property_{base_name}'
            renting_table_name = f'renting_{base_name}'

            if property_table_name in self.tables_data and renting_table_name in self.tables_data:
                property_df = self.tables_data[property_table_name]
                renting_df = self.tables_data[renting_table_name]

                merged_df = property_df.merge(renting_df, left_on='id', right_on='property_id', how='inner')
                merged_tables[f'merged_{base_name}'] = merged_df

                del self.tables_data[property_table_name]
                del self.tables_data[renting_table_name]

        self.tables_data.update(merged_tables)

    def perform_model_evaluation(self, df, table_name):
        X = df.drop(['realSum', "property_id", "id_x", "id_y",
                     "room_type", "room_shared", "room_private", "host_is_superhost", "biz"], axis=1)
        y = df['realSum']
        column_to_quad = ['attr_index_norm', 'rest_index_norm', "guest_satisfaction_overall"]
        for name in column_to_quad:
            X[name + '_squared'] = X[name] ** 2
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

        models = {
            "XGBoost": xgb.XGBRegressor(),
            "CatBoost": CatBoostRegressor(logging_level='Silent'),
            "randomForest": RandomForestRegressor(n_estimators=100),
            "linear_regression": LinearRegression(),
            "KNN": KNeighborsRegressor(n_neighbors=int(len(df) / 100))
        }
        min_mse = float("inf")
        best_model = None
        model_mse = {}

        for model_name, model in models.items():
            model.fit(X_train, y_train)
            y_pred = model.predict(X_test)
            mse = mean_squared_error(y_test, y_pred)
            model_mse[model_name] = mse
            logger.info(f"for table {table_name} the model {model_name} MSE: {mse}")

            if mse < min_mse:
                min_mse = mse
                best_model = model

        if best_model == models['linear_regression']:
            self.save_model_parameters(table_name, best_model.coef_, best_model.intercept_)
        return best_model, model_mse


if __name__ == "__main__":
    ds_handler = DsHandler()
    ds_handler.initialize_database()
    ds_handler.consume_data()
