# Create your models here.
import logging

from django.db import models
from django.utils.text import slugify

from cachedrequests.requesters import DataStoreRequest, etree
from xmltables.models import XmlBaseModel, XmlColumn, XmlField, XmlTable
from django.contrib.postgres.aggregates import ArrayAgg
from django.contrib.postgres.fields import JSONField
from typing import List
from collections import defaultdict
from decimal import Decimal

logger = logging.getLogger(__name__)


narrative_fields = """
SELECT 
    slugify(iati_identifier) AS "aims_identifier",
    "iati_identifier",
    "ordinality",
    "xmltable"."text",
    COALESCE("xmltable"."lang", "activity_lang") AS "lang",
    "ref",
    "type" 
FROM (
    SELECT
            TRIM(iati_identifier) AS iati_identifier, iati_version, xmltable.*
            FROM {iatiactivities_table}, xmltable('{row_expression}' PASSING {document_expression}
            COLUMNS
                "ordinality" FOR ORDINALITY,
                "content" xml PATH '.',
                {default_language_field},
                "ref" text PATH '../@ref',
                "type" text PATH '../@type'
        )
    ) parent, xmltable('//narrative' PASSING content
    COLUMNS
        "text" text PATH '.',
        "lang" text path '@xml:lang'
)
"""


class IatiActivities(models.Model):
    id = models.TextField(primary_key=True)
    iati_identifier = models.TextField()
    content = XmlField()

    # Parameters from the parent "iati-activities" file properties
    iati_version = models.DecimalField(max_digits=3, decimal_places=2)

    @classmethod
    def fetch(cls, params=None):
        params = params or {}
        for a in DataStoreRequest(params).activities():
            iati_identifier = a.find("iati-identifier").text.strip()
            content = etree.tostring(a)
            defaults = dict(
                id=slugify(iati_identifier),
                content=etree.tostring(a).decode(),
                iati_version=a.attrib["{http://datastore.iatistandard.org/ns}version"],
            )
            try:
                instance, created = cls.objects.update_or_create(
                    iati_identifier=iati_identifier, defaults=defaults
                )
            except Exception as e:
                logger.error(e)


class IatiCodelist(models.Model):
    """
    Lookup tables for IATI codelists
    """
    content = XmlField()
    iati_version = models.DecimalField(max_digits=3, decimal_places=2)
    
    # These are extracted from the XML
    name = models.TextField()
    complete = models.BooleanField()
    embedded = models.BooleanField()
    name = JSONField()
    description = JSONField()

    def save(self, *args, **kwargs):
        raise NotImplementedError


class NarrativeXmlTable(XmlTable):
    iati_version = models.DecimalField(max_digits=3, decimal_places=2, default=2.03)
    narrative_type = models.CharField(max_length=512)

    @property
    def default_language_field(self):
        if "iati-activity" not in self.row_expression:
            return "--"
        else:
            s = self.row_expression.split("/")
            return '"activity_lang" text PATH \'' + "../" * (len(s) - 2) + "@xml:lang'"

    @property
    def sql(self):
        s = ""
        s += narrative_fields.format(
            iatiactivities_table=IatiActivities._meta.db_table,
            document_expression='"content"',
            row_expression=self.row_expression,
            default_language_field=self.default_language_field,
        )
        s += f" WHERE iati_version = {self.iati_version}"
        return s

    def materialize(self):
        from django.db import connection

        name = slugify(f"{self.row_expression}{self.iati_version}".replace("-", "_"))
        with connection.cursor() as c:
            c.execute(f'DROP MATERIALIZED VIEW IF EXISTS "{name}" CASCADE')
            try:
                c.execute(f'CREATE MATERIALIZED VIEW "{name}" AS {self.sql}')
            except Exception as e:
                logger.error(f"""Unable to continue; SQL was {self.sql}""", exc_info=1)
                # continue


class IatiXmlTable(XmlTable):
    iati_version = models.DecimalField(max_digits=3, decimal_places=2, default=2.03)

    @property
    def sql(self):
        supersql = super().sql
        table = IatiActivities._meta.db_table
        return f"""
SELECT 
    iati_identifier,
    iati_version,
    xmltable.* 
FROM 
    {table}, 
    {supersql} 
WHERE {table}.iati_version = {self.iati_version}
"""

    def materialize(self):
        from django.db import connection

        name = slugify(f"{self.row_expression}{self.iati_version}".replace("-", "_"))
        with connection.cursor() as c:
            c.execute(f'DROP MATERIALIZED VIEW IF EXISTS "{name}" CASCADE')
            try:
                c.execute(f'CREATE MATERIALIZED VIEW "{name}" AS {self.sql}')
            except Exception as e:
                logger.error(f"""Unable to continue; SQL was {self.sql}""", exc_info=1)
                # continue
    def __str__(self):
        return '{} v{}'.format(super().__str__(), self.iati_version)


class IatiXmlColumnManager(models.Manager):
    def with_versions(self):
        return self.get_queryset().annotate(versions=ArrayAgg('xmltable__iatixmltable__iati_version'))

    def get_versions_for_column(self):
        """
        For individual column and row expressions
        show which IATI versions that particular XML
        path is valid for
        """
        return self.get_queryset().values('pk', 'column_expression', 'xmltable__row_expression').annotate(versions = ArrayAgg('xmltable__iatixmltable__iati_version'))

    def get_columns_for_versions(self, versions: List[Decimal]):
        """
        Return columns which are common to given IATI versions
        """
        self.get_versions_for_column().filter(versions__contains=versions)


class IatiXmlColumn(XmlColumn):
    objects = IatiXmlColumnManager()
