from django.contrib import admin
from .models import (
    Account,
    FileUpload,
    Transaction,
    Statement,
    LocationClassification,
    LocationSubClassification,
    TimeClassification,
    PersonClassification,
)

@admin.register(Account)
class AccountAdmin(admin.ModelAdmin):
    list_display = ('name', 'type', 'created_at')

@admin.register(FileUpload)
class FileUploadAdmin(admin.ModelAdmin):
    list_display = ('filename', 'account', 'status', 'transaction_count', 'created_at')
    list_filter = ('status', 'account')

@admin.register(Transaction)
class TransactionAdmin(admin.ModelAdmin):
    list_display = ('account', 'transaction_date', 'description', 'amount')
    list_filter = ('account', 'category')
    search_fields = ('description', 'description_2')
    
@admin.register(LocationClassification)
class LocationClassificationAdmin(admin.ModelAdmin):
    list_display = ('name', 'type')
    list_filter = ('type',)
    search_fields = ('name',)
    
@admin.register(LocationSubClassification)
class LocationSubClassificationAdmin(admin.ModelAdmin):
    list_display = ('name', 'location_classification')
    list_filter = ('location_classification',)
    search_fields = ('name',)
    
@admin.register(TimeClassification)
class TimeClassificationAdmin(admin.ModelAdmin):
    list_display = ('name',)
    list_filter = ('name',)
    search_fields = ('name',)
    
@admin.register(PersonClassification)
class PersonClassificationAdmin(admin.ModelAdmin):
    list_display = ('name',)
    list_filter = ('name',)
    search_fields = ('name',)

@admin.register(Statement)
class StatementAdmin(admin.ModelAdmin):
    list_display = ('account', 'period_start', 'period_end', 'opening_balance', 'closing_balance')
    list_filter = ('account', 'period_start', 'period_end')
    search_fields = ('account__name', 'period_start', 'period_end')
