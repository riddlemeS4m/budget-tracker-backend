from django.db import models

class Account(models.Model):
    TYPE_CHECKING = 'checking'
    TYPE_SAVINGS = 'savings'
    TYPE_CREDIT_CARD = 'credit_card'
    TYPE_INVESTMENT = 'investment'
    TYPE_LOAN = 'loan'
    TYPE_OTHER = 'other'

    TYPE_CHOICES = [
        (TYPE_CHECKING, 'Checking'),
        (TYPE_SAVINGS, 'Savings'),
        (TYPE_CREDIT_CARD, 'Credit Card'),
        (TYPE_INVESTMENT, 'Investment'),
        (TYPE_LOAN, 'Loan'),
        (TYPE_OTHER, 'Other'),
    ]

    name = models.CharField(max_length=255)
    type = models.CharField(max_length=255, choices=TYPE_CHOICES, default=TYPE_CHECKING)
    file_upload_schema = models.JSONField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.name


class FileUpload(models.Model):
    STATUS_PENDING = 'pending'
    STATUS_PROCESSING = 'processing'
    STATUS_COMPLETED = 'completed'
    STATUS_FAILED = 'failed'

    STATUS_CHOICES = [
        (STATUS_PENDING, 'Pending'),
        (STATUS_PROCESSING, 'Processing'),
        (STATUS_COMPLETED, 'Completed'),
        (STATUS_FAILED, 'Failed'),
    ]

    account = models.ForeignKey(Account, on_delete=models.CASCADE, related_name='file_uploads')
    filename = models.CharField(max_length=255)
    transaction_count = models.IntegerField(default=0)
    status = models.CharField(max_length=50, choices=STATUS_CHOICES, default=STATUS_PROCESSING)
    errors = models.TextField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.filename


class Transaction(models.Model):
    account = models.ForeignKey(Account, on_delete=models.CASCADE, related_name='transactions')
    file_upload = models.ForeignKey(FileUpload, on_delete=models.SET_NULL, null=True, blank=True, related_name='transactions')
    transaction_date = models.DateTimeField(null=True, blank=True)
    posted_date = models.DateTimeField(null=True, blank=True)
    description = models.CharField(max_length=255, null=True, blank=True)
    description_2 = models.CharField(max_length=255, null=True, blank=True)
    category = models.CharField(max_length=255, null=True, blank=True)
    subcategory = models.CharField(max_length=255, null=True, blank=True)
    amount = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    raw_data = models.JSONField()
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.account} - {self.transaction_date} - {self.description}"
