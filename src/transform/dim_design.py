"""This module contains the definition for transform_design()"""

import pandas as pd


def transform_design(design_data):
    """Function to transform data stored in ingestion bucket that was extracted
    from design table in totesys.

    Args:
        design_data (dict): dict of data from design file from
        ingestion bucket

    Returns:
        design_data_copy (dict): copy of design with data dict
        replaced by dataframe
    """
    design_data_copy = design_data.copy()
    row_data = design_data_copy["design"]

    for row in row_data:
        row.pop("created_at")
        row.pop("last_updated")

    design_data_copy["design"] = pd.DataFrame.from_records(row_data)

    design_data_copy["design"]["last_updated_date"] = "1970-01-01"
    design_data_copy["design"]["last_updated_time"] = "00:00"

    design_data_copy["design"]['last_updated_date'] = pd.to_datetime(
        design_data_copy["design"]['last_updated_date'], format="%Y-%m-%d").dt.date  # noqa
    design_data_copy["design"]['last_updated_time'] = pd.to_datetime(
        design_data_copy["design"]['last_updated_time'], format="%H:%M").dt.time  # noqa

    design_data_copy["design"].insert(
        0, 'design_record_id', range(
            1, len(
                design_data_copy["design"]) + 1))

    return design_data_copy
