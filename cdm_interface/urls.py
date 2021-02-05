""" URL Configuration for the cdm_interface app. """

__author__ = "William Tucker"
__date__ = "2019-10-03"
__copyright__ = "Copyright 2019 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level directory"


from django.urls import path
from django.shortcuts import redirect

from cdm_interface import views as interface_views


urlpatterns = [

#    path('select/', interface_views.SelectView.as_view()),
    path('select/', interface_views.SelectView.as_view()),
    path('v1/select/', interface_views.SelectView.as_view()),
    path('v1/constraints/<domain>', interface_views.ConstraintsView.as_view()),
#    path('wfs/', interface_views.RawWFSView.as_view()),
    

#    path('records/',
#        interface_views.LiteRecordView.as_view()),
#    path('records/<slug:index>/',
#        interface_views.LiteRecordView.as_view()),

    # Code tables:

    path('codetables/report_type/',
        interface_views.ReportTypeView.as_view()),
    path('codetables/report_type/<int:index>/',
        interface_views.ReportTypeView.as_view()),

    path('codetables/meaning_of_time_stamp/',
        interface_views.MeaningOfTimeStampView.as_view()),
    path('codetables/meaning_of_time_stamp/<int:index>/',
        interface_views.MeaningOfTimeStampView.as_view()),

    path('codetables/observed_variable/',
        interface_views.ObservedVariableView.as_view()),
    path('codetables/observed_variable/<int:index>/',
        interface_views.ObservedVariableView.as_view()),

    path('codetables/units/',
        interface_views.UnitsView.as_view()),
    path('codetables/units/<int:index>/',
        interface_views.UnitsView.as_view()),

    path('codetables/observation_value_significance/',
        interface_views.ObservationValueSignificanceView.as_view()),
    path('codetables/observation_value_significance/<int:index>/',
        interface_views.ObservationValueSignificanceView.as_view()),

    path('codetables/duration/',
        interface_views.DurationView.as_view()),
    path('codetables/duration/<int:index>/',
        interface_views.DurationView.as_view()),

    path('codetables/platform_type/',
        interface_views.PlatformTypeView.as_view()),
    path('codetables/platform_type/<int:index>/',
        interface_views.PlatformTypeView.as_view()),

    path('codetables/station_type/',
        interface_views.StationTypeView.as_view()),
    path('codetables/station_type/<int:index>/',
        interface_views.StationTypeView.as_view()),

    path('codetables/quality_flag/',
        interface_views.QualityFlagView.as_view()),
    path('codetables/quality_flag/<int:index>/',
        interface_views.QualityFlagView.as_view()),

    path('codetables/data_policy_licence/',
        interface_views.DataPolicyLicenceView.as_view()),
    path('codetables/data_policy_licence/<int:index>/',
        interface_views.DataPolicyLicenceView.as_view()),

]
