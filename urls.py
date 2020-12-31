from django.urls import include, path
from . import views

urlpatterns = [
    path("tables/", views.IatiXmlTableList.as_view(), name="iatixmltable-list"),
    path(
        "table/<pk>/",
        views.IatiXmlTableDetail.as_view(),
        name="iatixmltable-detail",
    ),
    path(
        "table/<pk>/content.json",
        views.IatiXmlTableJSON.as_view(),
        name="iatixmltable-detail-json",
    ),
    path(
        "iatiactivities.json",
        views.IatiActivities.as_view(),
        name="activities-json",
    ),
    path(
        "iatitransactions.json",
        views.IatiTransactions.as_view(),
        name="transactions-json",
    ),
    path(
        "participatingorganisations.json",
        views.IatiParticipatingOrganisation.as_view(),
        name="partorg-json",
    ),
]
