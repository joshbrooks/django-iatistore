from django.contrib import admin

from .models import (
    IatiActivities,
    IatiXmlColumn,
    IatiXmlTable,
    NarrativeXmlTable,
    IatiCodelist,
    IatiCodelistItem,
    IatiCodelistMapping,
)

# Register your models here.


@admin.register(IatiXmlColumn, IatiXmlTable, NarrativeXmlTable)
class IatiStoreAdmin(admin.ModelAdmin):
    readonly_fields = (("sql"),)


@admin.register(IatiActivities, IatiCodelist, IatiCodelistItem, IatiCodelistMapping)
class UnmodifiedAdmin(admin.ModelAdmin):
    pass

