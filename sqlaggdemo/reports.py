# coding=utf-8
from numbers import Number

from sqlagg import *
from sqlagg.views import *

from corehq.apps.reports.basic import BasicTabularReport, Column, GenericTabularReport
from corehq.apps.reports.datatables import DataTablesHeader, DataTablesColumnGroup, \
DataTablesColumn, DTSortType, DTSortDirection
from corehq.apps.reports.standard import ProjectReportParametersMixin, CustomProjectReport, DatespanMixin
from corehq.apps.reports.fields import DatespanField
from corehq.apps.groups.models import Group
from dimagi.utils.decorators.memoized import memoized
from sqlreport import SqlTabluarReport, Column, AggregateColumn, NO_VALUE


def username(report, key):
    return report.usernames_demo[key]


def combine_indicator(num, denom):
    if isinstance(num, Number) and isinstance(denom, Number):
        if denom != 0:
            return num * 100 / denom
        else:
            return 0
    else:
        return NO_VALUE


class DemoReport(SqlTabluarReport):
    name = "SQL Demo"
    slug = "sql_demo"
    filters = [
        "date > '{startdate}'",
        "date < '{enddate}'"
    ]
    groupings = ["user"]
    table_name = "user_table"
    database = "sqlagg-demo"

    @property
    def keys(self):
        return [["user1"], ["user2"]]

    @property
    @memoized
    def usernames_demo(self):
        return {"user1": "Joe", "user2": "Bob"}

    @property
    def filter_values(self):
        return {
            "startdate": self.datespan.startdate_param_utc,
            "enddate": self.datespan.enddate_param_utc
        }

    @property
    def columns(self):
        user = Column("Username", key="user", view_type=SimpleView, calculate_fn=username)
        i_a = Column("Indicator A", key="indicator_a")
        i_b = Column("Indicator B", key="indicator_b")

        agg_c_d = AggregateColumn("C/D", combine_indicator,
                                  Column("", key="indicator_c"),
                                  Column("", key="indicator_d"))

        return [
            user,
            i_a,
            i_b,
            agg_c_d
        ]