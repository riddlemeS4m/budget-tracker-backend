from rest_framework import serializers
from drf_spectacular.utils import extend_schema_field
from .models import (
    Account,
    FileUpload,
    Transaction,
    LocationClassification,
    LocationSubClassification,
    TimeClassification,
    PersonClassification,
    Statement,
)


class AccountSerializer(serializers.ModelSerializer):
    class Meta:
        model = Account
        fields = '__all__'


class FileUploadSerializer(serializers.ModelSerializer):
    # Read: full nested Account object
    account = AccountSerializer(read_only=True)
    # Write: accept an integer account ID
    account_id = serializers.PrimaryKeyRelatedField(
        queryset=Account.objects.all(),
        source='account',
        write_only=True,
    )
    # Computed from the first transaction's raw_data keys; always read-only
    headers = serializers.SerializerMethodField()

    class Meta:
        model = FileUpload
        fields = [
            'id', 'account', 'account_id', 'filename', 'headers',
            'transaction_count', 'status', 'errors', 'created_at', 'updated_at',
        ]

    @extend_schema_field(serializers.ListField(child=serializers.CharField()))
    def get_headers(self, obj):
        first_txn = obj.transactions.first()
        if not first_txn:
            return []
        return list(first_txn.raw_data.keys())


class LocationClassificationSerializer(serializers.ModelSerializer):
    transaction_count = serializers.IntegerField(read_only=True, default=0)
    subcategory_count = serializers.IntegerField(read_only=True, default=0)

    class Meta:
        model = LocationClassification
        fields = ['id', 'name', 'type', 'transaction_count', 'subcategory_count', 'created_at', 'updated_at']


class LocationSubClassificationSerializer(serializers.ModelSerializer):
    # Read: full nested LocationClassification object
    location_classification = LocationClassificationSerializer(read_only=True)
    # Write: accept an integer ID
    location_classification_id = serializers.PrimaryKeyRelatedField(
        queryset=LocationClassification.objects.all(),
        source='location_classification',
        write_only=True,
    )
    transaction_count = serializers.IntegerField(read_only=True, default=0)

    class Meta:
        model = LocationSubClassification
        fields = [
            'id', 'location_classification', 'location_classification_id',
            'name', 'transaction_count', 'created_at', 'updated_at',
        ]


class TimeClassificationSerializer(serializers.ModelSerializer):
    class Meta:
        model = TimeClassification
        fields = '__all__'


class PersonClassificationSerializer(serializers.ModelSerializer):
    class Meta:
        model = PersonClassification
        fields = '__all__'


class TransactionSerializer(serializers.ModelSerializer):
    # Read: full nested Account object
    account = AccountSerializer(read_only=True)
    # Write: accept an integer account ID
    account_id = serializers.PrimaryKeyRelatedField(
        queryset=Account.objects.all(),
        source='account',
        write_only=True,
    )
    # Read: full nested FileUpload object
    file_upload = FileUploadSerializer(read_only=True)
    # Write: accept an integer file upload ID (optional)
    file_upload_id = serializers.PrimaryKeyRelatedField(
        queryset=FileUpload.objects.all(),
        source='file_upload',
        write_only=True,
        allow_null=True,
        required=False,
    )

    class Meta:
        model = Transaction
        fields = [
            'id', 'account', 'account_id', 'file_upload', 'file_upload_id',
            'transaction_date', 'posted_date', 'description', 'description_2',
            'category', 'subcategory', 'amount', 'raw_data',
            'location_classification', 'location_subclassification',
            'time_classification', 'person_classification',
            'created_at', 'updated_at',
        ]


class TransactionBatchUpdateSerializer(serializers.Serializer):
    ids = serializers.ListField(child=serializers.IntegerField(), allow_empty=False)
    location_classification = serializers.PrimaryKeyRelatedField(
        queryset=LocationClassification.objects.all(),
        allow_null=True,
        required=False,
    )
    location_subclassification = serializers.PrimaryKeyRelatedField(
        queryset=LocationSubClassification.objects.all(),
        allow_null=True,
        required=False,
    )
    time_classification = serializers.PrimaryKeyRelatedField(
        queryset=TimeClassification.objects.all(),
        allow_null=True,
        required=False,
    )
    person_classification = serializers.PrimaryKeyRelatedField(
        queryset=PersonClassification.objects.all(),
        allow_null=True,
        required=False,
    )


class StatementSerializer(serializers.ModelSerializer):
    # Read: full nested Account object
    account = AccountSerializer(read_only=True)
    # Write: accept an integer account ID
    account_id = serializers.PrimaryKeyRelatedField(
        queryset=Account.objects.all(),
        source='account',
        write_only=True,
    )

    class Meta:
        model = Statement
        fields = [
            'id', 'account', 'account_id', 'period_start', 'period_end',
            'opening_balance', 'closing_balance', 'created_at', 'updated_at',
        ]
