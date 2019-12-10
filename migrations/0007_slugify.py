# Generated by Django 3.0 on 2019-12-09 05:50

from django.contrib.postgres.operations import UnaccentExtension
from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [("iatistore", "0006_iatiactivities_iati_version")]

    operations = [
        UnaccentExtension(),
        migrations.RunSQL(
            """
            CREATE OR REPLACE FUNCTION slugify("value" TEXT)
            RETURNS TEXT AS $$
              -- removes accents (diacritic signs) from a given string --
              WITH "unaccented" AS (
                SELECT unaccent("value") AS "value"
              ),
              -- lowercases the string
              "lowercase" AS (
                SELECT lower("value") AS "value"
                FROM "unaccented"
              ),
              -- remove single and double quotes
              "removed_quotes" AS (
                SELECT regexp_replace("value", '[''"]+', '', 'gi') AS "value"
                FROM "lowercase"
              ),
              -- replaces anything that's not a letter, number, hyphen('-'), or underscore('_') with a hyphen('-')
              "hyphenated" AS (
                SELECT regexp_replace("value", '[^a-z0-9\\-_]+', '-', 'gi') AS "value"
                FROM "removed_quotes"
              ),
              -- trims hyphens('-') if they exist on the head or tail of the string
              "trimmed" AS (
                SELECT regexp_replace(regexp_replace("value", '\-+$', ''), '^\-', '') AS "value"
                FROM "hyphenated"
              )
              SELECT "value" FROM "trimmed";
            $$ LANGUAGE SQL STRICT IMMUTABLE;"""
        ),
    ]
