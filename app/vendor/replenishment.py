import math

import pandas as pd
import scipy.stats as stats

from app.core.base import get_run_date
from app.core.utils.logging_ import get_logger

logger = get_logger(__name__)


# module function ------------------------------------------------------------------------
def run_min_max_service_level(
    input_df: pd.DataFrame, service_level_type: str, service_level_round: int
):
    """
    run min max service level from config table
    """
    values: list = []
    update_datetime = get_run_date(fmt="%Y-%m-%d %H:%M:%S")
    service_level_divide = 1 if service_level_type == "percent" else 100
    # input_values = input_df.values.tolist()
    input_values: list = [
        list(x)
        for x in zip(*(input_df[x].values.tolist() for x in input_df.columns))
    ]
    for value in input_values:
        result_version: int = int(value[-1])
        result_list: str = ", ".join(
            [
                str(
                    round(
                        stats.norm.ppf(float(_) / service_level_divide),
                        service_level_round,
                    )
                )
                for _ in value[:-1]
            ]
        )
        result_str: str = (
            f"({result_list}, {result_version}, 'Y', '{update_datetime}')"
        )
        values.append(result_str)

    result_values: str = (
        ", ".join([_ for _ in values if _ != ""])
        if any(_ != "" for _ in values)
        else ""
    )

    logger.info("Convert service level value will update to database")
    return result_values


# module function ------------------------------------------------------------------------
def run_prod_cls_criteria(input_df: pd.DataFrame):
    """
    run validate product class criteria value from config table
    """
    values: list = []
    update_datetime: str = get_run_date(fmt="%Y-%m-%d %H:%M:%S")
    input_values: list = [
        list(x)
        for x in zip(*(input_df[x].values.tolist() for x in input_df.columns))
    ]
    for value in input_values:
        result_version: int = int(value[-1])
        input_criteria = [float(_) for _ in value[:-1]]
        if not math.isclose(sum(input_criteria), 100) and not math.isclose(
            sum(input_criteria), 1
        ):
            input_criteria_filter: list = (
                [_ / 100 for _ in input_criteria]
                if sum(input_criteria) > 100
                else input_criteria
            )
            class_a, class_b, class_c = input_criteria_filter
            if math.isclose(
                (class_c - class_b) + (class_b - class_a) + class_a, 1
            ) and math.isclose(class_c, 1):
                result_list: str = ", ".join(
                    [str(class_a), str(class_b), str(class_c)]
                )
                result_str: str = (
                    f"({result_list}, {result_version}, 'Y', '{update_datetime}')"
                )
                values.append(result_str)
            else:
                values.append("")
        else:
            input_criteria_filter: list = (
                [_ / 100 for _ in input_criteria]
                if math.isclose(sum(input_criteria), 100)
                else input_criteria
            )
            class_a, class_b, class_c = input_criteria_filter
            if math.isclose(class_c, 1 - (class_b + class_a)):
                class_c: float = 1.0
                class_b: float = round(class_a + class_b, 10)
                result_list: str = ", ".join(
                    [str(class_a), str(class_b), str(class_c)]
                )
                result_str: str = (
                    f"({result_list}, {result_version}, 'Y', '{update_datetime}')"
                )
                values.append(result_str)
            else:
                values.append("")

    logger.info(
        "Validate product class criteria value success, it will update to database"
    )
    return (
        ", ".join([_ for _ in values if _ != ""])
        if any(_ != "" for _ in values)
        else ""
    )
