from django.contrib import admin
from .models import Account, FileUpload, Transaction

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
