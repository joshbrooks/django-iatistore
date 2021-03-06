# Generated by Django 3.0 on 2019-12-09 02:57

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("xmltables", "0001_initial"),
        ("iatistore", "0004_narrativexmltable_narrative_type"),
    ]

    operations = [
        migrations.CreateModel(
            name="IatiXmlColumn",
            fields=[
                (
                    "xmlcolumn_ptr",
                    models.OneToOneField(
                        auto_created=True,
                        on_delete=django.db.models.deletion.CASCADE,
                        parent_link=True,
                        primary_key=True,
                        serialize=False,
                        to="xmltables.XmlColumn",
                    ),
                )
            ],
            bases=("xmltables.xmlcolumn",),
        ),
        migrations.CreateModel(
            name="IatiXmlTable",
            fields=[
                (
                    "xmltable_ptr",
                    models.OneToOneField(
                        auto_created=True,
                        on_delete=django.db.models.deletion.CASCADE,
                        parent_link=True,
                        primary_key=True,
                        serialize=False,
                        to="xmltables.XmlTable",
                    ),
                ),
                (
                    "iati_version",
                    models.DecimalField(decimal_places=2, default=2.03, max_digits=3),
                ),
            ],
            bases=("xmltables.xmltable",),
        ),
    ]
