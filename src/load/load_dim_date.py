from sqlalchemy import create_engine

from src.transform.create_dim_date import create_dim_date


def load_dim_date():
    """Function to populate dim_date table in data warehouse
    """
    dim_date_df = create_dim_date("2010-01-01", "2050-12-31")

    db_user = "project_team_3"
    db_password = "OnvinNPtGz5zYR4P"
    db_host = "nc-data-eng-project-dw-prod.chpsczt8h1nu.eu-west-2.rds.amazonaws.com"  # noqa
    db_port = "5432"
    db_name = "postgres"

    db_url = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'  # noqa

    engine = create_engine(db_url)

    dim_date_df.to_sql(
        name="dim_date",
        con=engine,
        index=False,
        if_exists="append")

    engine.dispose()
