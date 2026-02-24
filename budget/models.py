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
    

class LocationClassification(models.Model):
    TYPE_INCOME = 'income'
    TYPE_EXPENSE = 'expense'
    TYPE_TRANSFER = 'transfer'
    TYPE_CHOICES = [
        (TYPE_INCOME, 'Income'),
        (TYPE_EXPENSE, 'Expense'),
        (TYPE_TRANSFER, 'Transfer'),
    ]

    name = models.CharField(max_length=255)
    type = models.CharField(max_length=50, choices=TYPE_CHOICES)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name_plural = 'location classifications'

    def __str__(self):
        return f"[{self.type}] {self.name}"


class LocationSubClassification(models.Model):
    location_classification = models.ForeignKey(LocationClassification, on_delete=models.CASCADE, related_name='location_subclassifications')
    name = models.CharField(max_length=255)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name_plural = 'location subclassifications'

    def __str__(self):
        return f"{self.location_classification.name} > {self.name}"


class TimeClassification(models.Model):
    name = models.CharField(max_length=255)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name_plural = 'time classifications'

    def __str__(self):
        return self.name


class PersonClassification(models.Model):
    name = models.CharField(max_length=255)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name_plural = 'person classifications'

    def __str__(self):
        return self.name


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
    location_classification = models.ForeignKey(LocationClassification, on_delete=models.SET_NULL, null=True, blank=True, related_name='transactions')
    location_subclassification = models.ForeignKey(LocationSubClassification, on_delete=models.SET_NULL, null=True, blank=True, related_name='transactions')
    time_classification = models.ForeignKey(TimeClassification, on_delete=models.SET_NULL, null=True, blank=True, related_name='transactions')
    person_classification = models.ForeignKey(PersonClassification, on_delete=models.SET_NULL, null=True, blank=True, related_name='transactions')
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.account} - {self.transaction_date} - {self.description}"
