from datetime import datetime
from dateutil.relativedelta import relativedelta
from collections import namedtuple

import enum
from typing import Tuple

_DateFormatType = namedtuple('DateFormatType', ['value', 'date_format'])
"""Tipos de formato de fecha."""


class EnumDateFormatTypes(enum.Enum):
    """Enumerado de tipos de formatos de fechas."""

    @property
    def date_format(self):
        return self.value.date_format

    YEAR_MONTH_DAY_HH_MM_SS = _DateFormatType(1, "%Y-%m-%d %H:%M:%S")


def get_current_year() -> int:
    """
    Devuelve el año actual
    :return: Año actual. Número entero.
    """
    return datetime.now().year


def get_current_date() -> datetime:
    """
    Devuelve la fecha actual.
    :return:
    """
    return datetime.now()


def get_date_for_n_years_ago(years: int) -> datetime:
    """
    Devuelve la fecha de hace n años desde el día actual.
    :return:
    """
    current_datetime = datetime.now()

    try:
        current_datetime = current_datetime.replace(year=current_datetime.year - years)
    except ValueError:
        current_datetime = current_datetime.replace(year=current_datetime.year - years, day=current_datetime.day - 1)
    return current_datetime


def get_date_six_months_ago() -> datetime:
    """
    Devuelve la fecha de hace seis meses desde el día actual.
    :return: Date
    """
    current_date = datetime.today()
    past_date = current_date - relativedelta(months=6)

    return past_date


def get_start_and_end_of_date(date) -> Tuple[datetime, datetime]:
    """
    Devuelve el principio y fin del día de una fecha.
    :param date:
    :return: Tupla.
    """
    since_date = datetime(year=date.year, month=date.month, day=date.day, hour=0, minute=0, second=0)
    till_date = datetime(year=date.year, month=date.month, day=date.day, hour=23, minute=59, second=59)

    return since_date, till_date


def get_start_and_end_of_year(year: int) -> Tuple[datetime, datetime]:
    """
    Devuelve una tupla siendo el primer valor el inicio del año y el segundo el final.
    :param year: Año del que obtener el principio y final.
    :return: tupla.
    """
    since_date = datetime(year=year, month=1, day=1, hour=0, minute=0, second=0)
    till_date = datetime(year=year, month=12, day=31, hour=23, minute=59, second=59)

    return since_date, till_date


def format_date(datetime_to_format: datetime, date_format: EnumDateFormatTypes) -> str:
    """
    Formatea la fecha a string.
    :param datetime_to_format: Fecha a formatear.
    :param date_format: Tipos de formato.
    :return: Fecha en formato str.
    """
    return datetime_to_format.strftime(date_format.date_format)


def timestamp_to_date(timestamp: float) -> datetime:
    """
    Devuelve un objeto fecha a partir de un timestamp.
    :param timestamp:
    :return:
    """
    return datetime.fromtimestamp(timestamp)


def string_to_datetime(date_time_str: str, date_format: EnumDateFormatTypes) -> datetime:
    """
    Devuelve un objeto fecha a partir de un timestamp.
    :param date_time_str: Fecha en formato string.
    :param date_format: Formato de fecha.
    :return: datetime
    """
    return datetime.strptime(date_time_str, date_format.date_format)


def string_to_datetime_sql(date_time_str: str) -> datetime:
    """
    Devuelve un objeto fecha a partir de un timestamp.
    :param date_time_str: Fecha en formato string. El string se asume que está en formato de
    fecha SQL: YYYY-MM-dd HH:mm:ss
    :return: datetime
    """
    return datetime.strptime(date_time_str, EnumDateFormatTypes.YEAR_MONTH_DAY_HH_MM_SS.date_format)
