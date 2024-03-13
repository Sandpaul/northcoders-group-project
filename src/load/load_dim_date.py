import json
import boto3

from sqlalchemy import create_engine

from src.transform.create_dim_date import create_dim_date


def load_dim_date():
    """Function to populate dim_date table in data warehouse
    """
    dim_date_df = create_dim_date("2010-01-01", "2050-12-31")

    sm = boto3.client("secretsmanager")

    dw_secret = sm.get_secret_value(SecretId="dw_credentials")
    dw_credentials = dw_secret["SecretString"]
    dw_dict = json.loads(dw_credentials)

    engine = create_engine(f'postgresql+pg8000://{dw_dict["user"]}:{dw_dict["password"]}@{dw_dict["host"]}:{dw_dict["port"]}/{dw_dict["database"]}')  # noqa

    with engine.connect() as connection:

        connection.begin()

        try:

            dim_date_df.to_sql(
            name="dim_date",
            con=engine,
            index=False,
            if_exists="append")

            connection.commit()

        except Exception as e:

            connection.rollback()

            print(f"An error occurred: {e}")

            raise

        finally:

            connection.close()
