# Generated by Django 3.0 on 2020-01-17 03:10

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("iatistore", "0011_iaticodelistmapping"),
    ]

    operations = [
        migrations.AlterField(
            model_name="iaticodelistmapping",
            name="iati_version",
            field=models.DecimalField(
                blank=True, decimal_places=2, max_digits=3, null=True, unique=True
            ),
        ),
    ]
