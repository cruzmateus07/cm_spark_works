import pyspark.sql.functions as func


class SparkUtils():

    def get_last_update_ts(self, df: DataFrame, ts_field: str):
        last_update_ts = df.select(func.max(col(ts_field))).collect()[0][0]
        print(f"max value for {ts_field} is {last_update_ts}")

        return last_update_ts
