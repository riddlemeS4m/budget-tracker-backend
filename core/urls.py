"""
URL configuration for core project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/6.0/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path, include
from drf_spectacular.views import SpectacularAPIView, SpectacularSwaggerView, SpectacularRedocView
from rest_framework.routers import DefaultRouter
from budget.views import (
    AccountViewSet,
    FileUploadViewSet,
    TransactionListView,
    TransactionDetailView,
    TransactionExportView,
    LocationClassificationViewSet,
    LocationSubClassificationViewSet,
    TimeClassificationViewSet,
    PersonClassificationViewSet,
    StatementViewSet,
    CashFlowStatementViewSet,
)

router = DefaultRouter()
router.register(r'accounts', AccountViewSet)
router.register(r'file-uploads', FileUploadViewSet)
router.register(r'location-classifications', LocationClassificationViewSet)
router.register(r'location-subclassifications', LocationSubClassificationViewSet)
router.register(r'time-classifications', TimeClassificationViewSet)
router.register(r'person-classifications', PersonClassificationViewSet)
router.register(r'statements', StatementViewSet)
router.register(r'reports/cash-flow-statement', CashFlowStatementViewSet, basename='cash-flow-statement-report')

api_v1_patterns = [
    path('', include(router.urls)),
    path('transactions/', TransactionListView.as_view(), name='transaction-list'),
    path('transactions/export/', TransactionExportView.as_view(), name='transaction-export'),
    path('transactions/<int:pk>/', TransactionDetailView.as_view(), name='transaction-detail'),
    
    # Schema and docs
    path('schema/', SpectacularAPIView.as_view(), name='schema'),
    path('docs/', SpectacularSwaggerView.as_view(url_name='schema'), name='swagger-ui'),
    path('redoc/', SpectacularRedocView.as_view(url_name='schema'), name='redoc'),
]

urlpatterns = [
    path('admin/', admin.site.urls),
    path('api/v1/', include(api_v1_patterns)),
]
