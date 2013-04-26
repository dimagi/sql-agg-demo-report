# coding=utf-8
from numbers import Number
from corehq.apps.reports.fields import DatespanField
from corehq.apps.reports.standard import DatespanMixin

from sqlagg import *
from sqlagg.columns import *

from dimagi.utils.decorators.memoized import memoized
from sqlreport import SqlTabularReport, Column, AggregateColumn, NO_VALUE


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


class DemoReport(SqlTabularReport, DatespanMixin):
    name = "SQL Demo"
    slug = "sql_demo"
    field_classes = (DatespanField,)
    datespan_default_days = 30
    filters = [
        "date > :startdate",
        "date < :enddate"
    ]
    # still need to sort out multi-level groupings. Query works fine but probably need a new template / base report
    group_by = None#["user"]
    table_name = "user_table"
    # database can be overridden here but the default is the project domain
    database = "sqlagg-demo"

    @property
    def keys(self):
        # would normally be loaded by couch
        #return [["user1"], ["user2"], ['user3']]
        return None

    @property
    @memoized
    def usernames_demo(self):
        # would normally be loaded by couch
        return {"user1": "Joe", "user2": "Bob", 'user3': "Gill"}

    @property
    def filter_values(self):
        return {
            "startdate": self.datespan.startdate_param_utc,
            "enddate": self.datespan.enddate_param_utc
        }

    @property
    def columns(self):
        user = Column("Username", "user", column_type=SimpleColumn, calculate_fn=username)
        i_a = Column("Indicator A", "indicator_a")
        i_b = Column("Indicator B", "indicator_b")

        agg_c_d = AggregateColumn("C/D", combine_indicator,
                                  SumColumn("indicator_c"),
                                  SumColumn("indicator_d"))

        return [
            #user,
            i_a,
            i_b,
            agg_c_d
        ]