from django.shortcuts import render
from django.views.generic import View, ListView, DetailView
from django.http import JsonResponse
from . import models as iatixmltables
from django.db import connection


class IatiXmlTableList(ListView):
    model = iatixmltables.IatiXmlTable


class IatiXmlTableDetail(DetailView):
    context_object_name = "xmltable"
    queryset = iatixmltables.IatiXmlTable.objects.all()

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        # Use a materialized view
        context["headers"], context["rows"] = kwargs["object"].execute_with_columns()
        return context


class IatiXmlTableJSON(DetailView):
    queryset = iatixmltables.IatiXmlTable.objects.all()

    def get(self, *args, **kwargs):
        with connection.cursor() as c:
            c.execute(self.get_object().sql)
            columns = [col[0] for col in c.description]
            return JsonResponse(
                dict(results=[dict(zip(columns, row)) for row in c.fetchall()])
            )


class IatiActivities(View):
    """
    Returns all fields common to IATI versions 2.01, 2.02 and 2.03
    from the materialized views

    The matviews need to be created first
    """

    def get(self, *args, **kwargs):
        tables = iatixmltables.IatiXmlTable.objects.filter(
            row_expression="/iati-activity"
        )

        # On error
        # [
        #     i.matview_create()
        #     for i in iatixmltables.IatiXmlTable.objects.filter(
        #         row_expression="/iati-activity"
        #     )
        # ]

        # List of distinct column names for versions
        columns = list(
            iatixmltables.IatiXmlColumn.objects.get_versions_for_column(
                "/iati-activity"
            )
            .exclude(col_name__startswith="fss")
            .exclude(col_name__startswith="crs")
            .filter(versions__contains=[2.03, 2.01, 2.02])
            .values_list("col_name", flat=True)
            .distinct()
        )

        columns_join = ", ".join(columns)

        sql = " UNION ".join(
            [
                f"SELECT iati_identifier, iati_version, {columns_join} FROM {name}"
                for name in [t.table_name for t in tables]
            ]
        )

        with connection.cursor() as c:
            c.execute(sql)
            columns = [col[0] for col in c.description]
            return JsonResponse(
                dict(results=[dict(zip(columns, row)) for row in c.fetchall()])
            )


class IatiTransactions(View):
    """
    Returns all fields common to IATI versions 2.01, 2.02 and 2.03
    from the materialized views

    The matviews need to be created first
    """

    def get(self, *args, **kwargs):
        tables = iatixmltables.IatiXmlTable.objects.filter(
            row_expression="/iati-activity/transaction"
        )

        # columns = list(
        #     iatixmltables.IatiXmlColumn.objects.get_versions_for_column(
        #         "/iati-activity/transaction"
        #     )
        #     .filter(versions__contains=[2.03, 2.01, 2.02])
        #     .values_list("col_name", flat=True)
        #     .distinct()
        # )

        columns = [
            "value",
            "value_currency",
            "value_value_date",
            "transaction_type_code",
        ]

        columns_join = ", ".join(columns)

        sql = " UNION ".join(
            [
                f"SELECT iati_identifier, iati_version, {columns_join} FROM {name}"
                for name in [t.table_name for t in tables]
            ]
        )

        with connection.cursor() as c:
            c.execute(sql)
            columns = [col[0] for col in c.description]
            return JsonResponse(
                [dict(zip(columns, row)) for row in c.fetchall()],
                safe=False,
                json_dumps_params={"indent": 1},
            )
