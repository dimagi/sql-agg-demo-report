# coding=utf-8
import sqlalchemy
from sqlagg import *
from sqlagg.views import *

from corehq.apps.reports.basic import BasicTabularReport, Column, GenericTabularReport
from corehq.apps.reports.datatables import DataTablesHeader, DataTablesColumnGroup, \
    DataTablesColumn, DTSortType, DTSortDirection
from corehq.apps.reports.standard import ProjectReportParametersMixin, CustomProjectReport, DatespanMixin
from corehq.apps.reports.fields import DatespanField
from corehq.apps.groups.models import Group
from dimagi.utils.decorators.memoized import memoized
from django.conf import settings

No_VALUE = "--"


class Column(object):
    def __init__(self, name, calculate_fn=None, view_type=None, *args, **kwargs):
        view_args = (
            # args specific to KeyView constructor
            'key', 'table_name', 'group_by', 'filters'
        )
        view_kwargs = {}

        for arg in view_args:
            try:
                view_kwargs[arg] = kwargs.pop(arg)
            except KeyError:
                pass

        view_type = view_type or SumView

        if 'key' in view_kwargs:
            if 'sort_type' not in kwargs:
                kwargs['sort_type'] = DTSortType.NUMERIC
                kwargs['sortable'] = True

            key = view_kwargs.pop('key')
            self.view = view_type(key, **view_kwargs)

        self.calculate_fn = calculate_fn
        self.group = kwargs.pop('group', None)

        self.data_tables_column = DataTablesColumn(name, *args, **kwargs)
        if self.group:
            self.group.add_column(self.data_tables_column)

    def get_value(self, report, row, row_key):
        if isinstance(self.view, SimpleView):
            return self.calculate_fn(report, row_key) if self.calculate_fn else row_key
        else:
            return (self.view.get_value(row) if row else None) or NO_VALUE


class AggregateColumn(object):
    def __init__(self, name, aggregate_fn, *args, **kwargs):
        self.aggregate_fn = aggregate_fn
        self.columns = args
        self.name = name

        self.group = kwargs.pop('group', None)
        self.data_tables_column = DataTablesColumn(name, **kwargs)
        if self.group:
            self.group.add_column(self.data_tables_column)

        self.view = AggregateView(aggregate_fn, *[c.view for c in self.columns])

    def get_value(self, report, row, row_key):
        return (self.view.get_value(row) if row else None) or NO_VALUE


class AggregateView(object):
    def __init__(self, aggregate_fn, *views):
        self.aggregate_fn = aggregate_fn
        self.views = views

    def get_value(self, row):
        return self.aggregate_fn(*[v.get_value(row) for v in self.views])

    def apply_vc(self, view_context):
        for view in self.views:
            view.apply_vc(view_context)


class SqlTabluarReport(GenericTabularReport, CustomProjectReport, ProjectReportParametersMixin, DatespanMixin):
    field_classes = (DatespanField,)
    datespan_default_days = 30
    exportable = True

    @property
    def columns(self):
        """
        Returns a list of Columns
        """
        raise NotImplementedError()

    @property
    def groupings(self):
        """
        Returns a list of 'group by' column names
        """
        raise NotImplementedError()

    @property
    def filters(self):
        """
        Returns a list of filter statements e.g. ["date > '{enddate}'"]
        """
        raise NotImplementedError()

    @property
    def filter_values(self):
        """
        Return a dict mapping the filter keys to actual values e.g. {"enddate": date(2013,01,01)}
        """
        raise NotImplementedError()

    @property
    def keys(self):
        """
        The list of report keys (e.g. users) or None to just display all the data returned from the query
        """
        raise NotImplementedError()

    @property
    def columns(self):
        raise NotImplementedError()

    @property
    def fields(self):
        return [cls.__module__ + '.' + cls.__name__
                for cls in self.field_classes]

    @property
    def headers(self):
        return DataTablesHeader(*[c.data_tables_column for c in self.columns])

    @property
    @memoized
    def view_context(self):
        return ViewContext(self.table_name, self.filters, self.groupings)

    @property
    def rows(self):
        vc = self.view_context
        for c in self.columns:
            c.view.apply_vc(vc)
        engine = sqlalchemy.create_engine(settings.SQL_REPORTING_DATABASE.format("domain"=self.domain))  # "postgresql+psycopg2://postgres:password@localhost/care_benin")
        conn = engine.connect()
        vc.resolve(conn, self.filter_values)

        """
        TODO: support getting rows out of multi-group data
        e.g.
        {
            "provinceA": {
                "district1": {
                    "col1": val1,
                    "col2": val2
                }
            }
        }
        """
        if self.keys:
            for key_group in self.keys:
                for key in key_group:
                    yield [c.get_value(self, vc.data.get(key, None), key) for c in self.columns]
        else:
            for k, v in vc.data.items():
                yield [c.get_value(self, vc.data.get(k), k) for c in self.columns]
