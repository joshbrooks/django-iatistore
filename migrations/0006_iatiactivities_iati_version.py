# Generated by Django 3.0 on 2019-12-09 03:41

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [("iatistore", "0005_iatixmlcolumn_iatixmltable")]

    operations = [
        migrations.AddField(
            model_name="iatiactivities",
            name="iati_version",
            field=models.DecimalField(decimal_places=2, default=2.02, max_digits=3),
            preserve_default=False,
        )
    ]
