from datetime import datetime
from dateutil import tz
from_zone = tz.gettz('UTC')
# TODO: temporary utc +10 - airflow's execution_date is past 1 hour
to_zone = tz.gettz('Australia/Brisbane')
# TODO: need this for manual run
to_kor_zone = tz.gettz('Asia/Seoul')


def utc_to_kst(input_date):
    utc = datetime.strptime(input_date, '%Y%m%d%H')
    utc = utc.replace(tzinfo=from_zone)
    seoul = utc.astimezone(to_zone)
    return datetime.strftime(seoul, '%Y%m%d%H')


def utc_to_kst_date(input_date):
    utc = datetime.strptime(input_date, '%Y%m%d%H')
    utc = utc.replace(tzinfo=from_zone)
    seoul = utc.astimezone(to_zone)
    return datetime.strftime(seoul, '%Y%m%d')


def get_kst_last_hour(input_date):
    utc = datetime.strptime(input_date, '%Y%m%d%H')
    utc = utc.replace(tzinfo=from_zone)
    seoul = utc.astimezone(to_kor_zone)
    return datetime.strftime(seoul, '%Y%m%d%H')


class Params(object):
    def __repr__(self):
        return str(self.__dict__)