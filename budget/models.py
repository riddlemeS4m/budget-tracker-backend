from django.db import models

class Account(models.Model):
    name = models.CharField(max_length=255)
    type = models.CharField(max_length=255)
    file_upload_schema = models.JSONField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.name


class FileUpload(models.Model):
    account = models.ForeignKey(Account, on_delete=models.CASCADE, related_name='file_uploads')
    filename = models.CharField(max_length=255)
    transaction_count = models.IntegerField()
    status = models.CharField(max_length=50)
    errors = models.TextField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.filename


class Transaction(models.Model):
    account = models.ForeignKey(Account, on_delete=models.CASCADE, related_name='transactions')
    file_upload = models.ForeignKey(FileUpload, on_delete=models.CASCADE, related_name='transactions')
    transaction_date = models.DateTimeField(null=True, blank=True)
    posted_date = models.DateTimeField(null=True, blank=True)
    description = models.CharField(max_length=255, null=True, blank=True)
    description_2 = models.CharField(max_length=255, null=True, blank=True)
    category = models.CharField(max_length=255, null=True, blank=True)
    amount = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    raw_data = models.JSONField()
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.account} - {self.transaction_date} - {self.description}"
