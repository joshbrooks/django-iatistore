from django.shortcuts import render
from django.views.generic import View, ListView, DetailView
from django.http import JsonResponse, HttpResponse
from . import models as iatixmltables
from . import transaction_pb2
from django.db import connection
from collections import defaultdict


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

        columns = [
            "value",
            "value_currency",
            "value_value_date",
            "transaction_type_code",
            "ref",
        ]

        columns_join = ", ".join(columns)

        sql = " UNION ".join(
            [
                f"SELECT iati_identifier, iati_version, {columns_join} FROM {name}"
                for name in [t.table_name for t in tables]
            ]
        )

        activities = transaction_pb2.ActivityTransactionList()

        with connection.cursor() as c:

            c.execute(sql)
            by_id = defaultdict(list)
            versions = {}
            for row in c.fetchall():
                transaction = dict(
                    value=float(row[2]),
                    currency=row[3],
                    datestamp=int(row[4].replace("-", "")),
                    transaction_type_code=str(row[5]),
                    id=row[6],
                    activity=row[0],
                )

                by_id[row[0]].append(transaction)
                if row[0] not in versions:

                    versions[row[0]] = "V%d" % (row[1] * 100)

            for act_id, ts in by_id.items():
                act = activities.activities.add()
                for t in ts:
                    transaction = act.transactions.add()
                    for field in t.keys():
                        value = t.get(field)
                        if value:
                            setattr(transaction, field, value)
                    print(transaction.SerializeToString())

        return HttpResponse(
            activities.SerializeToString(), content_type="application/octet-stream"
        )


class IatiParticipatingOrganisation(View):
    def get(self, *args, **kwargs):

        tables = iatixmltables.IatiXmlTable.objects.filter(
            row_expression="/iati-activity/participating-org"
        )

        columns = ["type", "ref", "role"]

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
