# Create your models here.
import logging

from django.db import models, connection
from django.db.utils import ProgrammingError
from django.utils.text import slugify

from importlib import resources
from iatistore import iatisql
from cachedrequests.requesters import (
    DataStoreRequest,
    etree,
    CodelistRequest,
    CodelistMappingRequest,
)
from requests.exceptions import HTTPError
from xmltables.models import XmlColumn, XmlField, XmlTable
from django.contrib.postgres.aggregates import ArrayAgg
from django.db.models import JSONField
from typing import List
from collections import defaultdict
from decimal import Decimal
from django.conf import settings

iati_versions = getattr(settings, "IATI_VERSIONS")
iati_version = getattr(settings, "DEFAULT_IATI_VERSION")

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
    """
    The main data source for each Activity
    """

    id = models.TextField(primary_key=True)
    iati_identifier = models.TextField()
    content = XmlField()

    # Parameters from the parent "iati-activities" file properties
    iati_version = models.DecimalField(max_digits=3, decimal_places=2)

    @classmethod
    def fetch(cls, params=None):
        """
        For Uzbekistan (MIFT-AIMS load)
        IatiActivities.fetch(params = [('recipient-country', 'UZ'),('stream', 'True')])
        """
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

    def __str__(self):
        return f"{self.iati_identifier}"


class IatiCodelistMapping(models.Model):
    content = XmlField(null=True)
    iati_version = models.DecimalField(
        max_digits=3, decimal_places=2, null=True, blank=True, unique=True
    )

    # Fetch on save
    def save(self, *args, **kwargs):
        if not self.content:
            self.content = (
                CodelistMappingRequest(version=self.iati_version).content().decode()
            )
        super().save(*args, **kwargs)

    @classmethod
    def update_mappings(cls, iati_version: Decimal = 2.03):
        cls.objects.get_or_create(iati_version=iati_version)[0].save()

    @classmethod
    def update_mappings_all(cls):
        versions = getattr(settings, "IATI_VERSIONS")
        for v in versions:
            cls.update_mappings(iati_version=v)


class CodelistMappingXmlColumn(XmlColumn):
    """
    This ought to have path, codelist, and condition attributes.
    This plus the content of IatiCodelistMapping permits
    us to determine which lookup applies to which attribute.
    """

    pass


class IatiCodelist(models.Model):
    """
    Lookup tables for IATI codelists
    """

    EMBEDDED_CODELISTS = {
        "ActivityDateType",
        "ActivityStatus",
        "BudgetStatus",  # This one for 2.02+
        "BudgetType",
        "DocumentCategory",
        "GazetteerAgency",
        "OrganisationRole",
        "RelatedActivityType",
        "TransactionType",
    }

    NONEMBEDDED_CODELISTS = {
        "ActivityScope",
        "AidType-category",
        "AidType",
        "AidTypeVocabulary",
        "BudgetIdentifier",
        "BudgetIdentifierSector-category",
        "BudgetIdentifierSector",
        "BudgetIdentifierVocabulary",
        "BudgetNotProvided",
        "CRSAddOtherFlags",
        "CRSChannelCode",
        "CashandVoucherModalities",
        "CollaborationType",
        "ConditionType",
        "ContactType",
        "Country",
        "Currency",
        "DescriptionType",
        "DisbursementChannel",
        "DocumentCategory-category",
        "EarmarkingCategory",
        "FileFormat",
        "FinanceType-category",
        "FinanceType",
        "FlowType",
        "GeographicExactness",
        "GeographicLocationClass",
        "GeographicLocationReach",
        "GeographicVocabulary",
        "GeographicalPrecision",
        "HumanitarianScopeType",
        "HumanitarianScopeVocabulary",
        "IATIOrganisationIdentifier",
        "IndicatorMeasure",
        "IndicatorVocabulary",
        "Language",
        "LoanRepaymentPeriod",
        "LoanRepaymentType",
        "LocationType-category",
        "LocationType",
        "OrganisationIdentifier",
        "OrganisationRegistrationAgency",
        "OrganisationType",
        "OtherIdentifierType",
        "PolicyMarker",
        "PolicyMarkerVocabulary",
        "PolicySignificance",
        "PublisherType",
        "Region",
        "RegionVocabulary",
        "ResultType",
        "ResultVocabulary",
        "Sector",
        "SectorCategory",
        "SectorVocabulary",
        "TagVocabulary",
        "TiedStatus",
        "UNSDG-Goals",
        "UNSDG-Targets",
        "VerificationStatus",
        "Version",
    }

    content = XmlField(null=True)
    iati_version = models.DecimalField(
        max_digits=3, decimal_places=2, null=True, blank=True
    )

    # These are extracted from the XML
    label = models.TextField()
    complete = models.BooleanField()
    embedded = models.BooleanField(default=False)

    # Language code: Text value fields
    name = JSONField(blank=True, null=True)
    description = JSONField(blank=True, null=True)

    def __str__(self):
        return f"{self.label} {self.iati_version}"

    class Meta:
        unique_together = [["iati_version", "label"]]

    @staticmethod
    def _set_names():
        """
        Import the code from the "iatisql" directory
        and runthe functions there to update the name and description fields
        """

        with connection.cursor() as c:
            c.execute(resources.read_text(iatisql, "codelist_name.sql"))
            c.execute(resources.read_text(iatisql, "codelist_description.sql"))

    def save(self, *args, **kwargs):
        if not self.content:
            logger.info(f"Fetching codelist {self.label} for {self.iati_version}")
            try:
                response = CodelistRequest(
                    version=self.iati_version,
                    codelist_name=self.label,
                    embedded=self.embedded,
                )
                content = response.as_xml()
            except HTTPError:
                logger.warn("Appears not to exist")
                return
            self.content = etree.tostring(content).decode()
            assert self.label == content.attrib.get("name")
            self.complete = (
                True if content.attrib.get("complete", None) == "1" else False
            )
        super().save(*args, **kwargs)

    @classmethod
    def fetch_all(cls):
        for v in iati_versions:
            logger.info(f"Fetching codelists for {v}")
            for label in cls.EMBEDDED_CODELISTS:
                try:
                    instance = cls.objects.get(iati_version=v, label=label).save()
                except cls.DoesNotExist:
                    cls(iati_version=v, label=label, embedded=True).save()

            for label in cls.NONEMBEDDED_CODELISTS:
                try:
                    instance = cls.objects.get(iati_version=v, label=label).save()
                except cls.DoesNotExist:
                    cls(iati_version=v, label=label, embedded=False).save()

    def update_items(self):
        pass


class IatiCodelistItem(models.Model):
    """
    Fragments of "Codelist" xml live here
    """

    code = models.TextField()
    name = JSONField(blank=True, null=True)
    description = JSONField(blank=True, null=True)

    # Category and IATI Version
    # taken together are a composite FK to
    # IatiCodelist
    codelist = models.ForeignKey(IatiCodelist, on_delete=models.CASCADE)
    url = models.URLField(null=True, blank=True)
    # public_database =
    status = models.TextField(null=True, blank=True)
    activation_date = models.DateField(null=True, blank=True)
    withdrawal_date = models.DateField(null=True, blank=True)

    class Meta:
        unique_together = [["codelist", "code"]]
        indexes = [models.Index(fields=["codelist", "code"])]

    def __str__(self):
        default_language = "en"
        name = self.name.get(default_language) if self.name else "Unnamed"
        return f"{self.code} {name}"


class NarrativeXmlTable(XmlTable):
    iati_version = models.DecimalField(
        max_digits=3, decimal_places=2, default=iati_version
    )
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
    iati_version = models.DecimalField(
        max_digits=3, decimal_places=2, default=iati_version
    )

    @property
    def table_name(self):
        return slugify(f"{self.row_expression}{self.iati_version}".replace("-", "_"))

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

    # TODO: Refactor this into a reusable thing
    def matview_create(self):
        with connection.cursor() as c:
            try:
                c.execute(f'CREATE MATERIALIZED VIEW "{self.table_name}" AS {self.sql}')
            except Exception as e:
                logger.error(f"""Unable to continue; SQL was {self.sql}""", exc_info=1)

    def matview_drop(self):
        name = slugify(f"{self.row_expression}{self.iati_version}".replace("-", "_"))
        with connection.cursor() as c:
            c.execute(f'DROP MATERIALIZED VIEW IF EXISTS "{self.table_name}" CASCADE')

    def execute(self):
        """
        Override the parent behaviour to pull from materialilzed view
        """
        with connection.cursor() as c:
            c.execute(f"SELECT * FROM {self.table_name}")
            return c.fetchall()

    def execute_with_columns(self, auto_create=True):
        """
        Override the parent behaviour to pull from materialilzed view
        """
        with connection.cursor() as c:
            try:
                c.execute(f"SELECT * FROM {self.table_name}")
            except ProgrammingError:
                self.matview_create()
                return self.execute_with_columns(auto_create=False)

            return [col[0] for col in c.description], c.fetchall()

    def __str__(self):
        return "{} v{}".format(super().__str__(), self.iati_version)


class IatiXmlColumnManager(models.Manager):
    def with_versions(self):
        return self.get_queryset().annotate(
            versions=ArrayAgg("xmltable__iatixmltable__iati_version")
        )

    def get_versions_for_column(self, row_expression):
        """
        For individual column and row expressions
        show which IATI versions that particular XML
        path is valid for
        """
        return (
            self.get_queryset()
            .filter(xmltable__row_expression=row_expression)
            .values("pk", "column_expression")
            .annotate(versions=ArrayAgg("xmltable__iatixmltable__iati_version"))
        )

    def get_columns_for_versions(self, versions: List[Decimal], row_expression: str):
        """
        Return columns which are common to given IATI versions
        """
        return self.get_versions_for_column(row_expression).filter(
            versions__contains=versions
        )

    def get_common_fields(self, row_expression: str):
        """
        Which fields are valid for every IATI version
        in settings
        """
        return self.get_columns_for_versions(
            versions=iati_versions, row_expression=row_expression
        )


class IatiXmlColumn(XmlColumn):
    objects = IatiXmlColumnManager()
