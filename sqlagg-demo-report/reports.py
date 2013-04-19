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
from sqlreport import SqlTabluarReport, Column, AggregateColumn, No_VALUE


def groupname(report, key):
    return report.groupnames[key]


def combine_indicator(num, denom):
    if isinstance(num, Number) and isinstance(denom, Number):
        return num * 100 / denom
    else:
        return NO_VALUE


class DemoReport(SqlTabluarReport):
    name = "SQL Demo"
    slug = "sql_demo"
    filters = [
        "date > '{startdate}'",
        "date < '{enddate}'"
    ]
    groupings = ["village"]
    table_name = "care1"

    @property
    def keys(self):
        for group in self.groups:
            yield [group._id]

    @property
    @memoized
    def groups(self):
        return [g for g in Group.by_domain(self.domain) if not g.reporting]

    @property
    @memoized
    def groupnames(self):
        return dict([(group._id, group.name) for group in self.groups])

    @property
    def filter_values(self):
        return {
            "startdate": self.datespan.startdate_param_utc,
            "enddate": self.datespan.enddate_param_utc
        }

    @property
    def columns(self):
        village = Column("Village", key="village", view_type=SimpleView, calculate_fn=groupname)
        birth_place_hopital = Column("Birth place hospital", key="birth_place_hopital")
        birth_place_mat_isolee = Column("birth place isolee", key="birth_place_mat_isolee")
        referrals_open_30_days = Column("Referrals open long", key="referrals_open_30_days", filters=["date < '{enddate}'"])

        test_agg = AggregateColumn("Test aggregate", combine_indicator,
                                    Column("", key="birth_one_month_ago_followup_x0"),
                                    Column("", key="birth_one_month_ago"))

        return [
            village,
            birth_place_hopital,
            birth_place_mat_isolee,
            referrals_open_30_days,
            test_agg
        ]