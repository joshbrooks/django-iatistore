from django.contrib import admin

from .models import (IatiActivities, IatiXmlColumn, IatiXmlTable,
                     NarrativeXmlTable)

# Register your models here.



@admin.register(IatiXmlColumn, IatiXmlTable, NarrativeXmlTable)
class IatiStoreAdmin(admin.ModelAdmin):
    readonly_fields = (("sql"),)
