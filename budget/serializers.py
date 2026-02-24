from rest_framework import serializers
from .models import Account, FileUpload, Transaction, LocationClassification, LocationSubClassification, TimeClassification, PersonClassification


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

    def get_headers(self, obj):
        first_txn = obj.transactions.first()
        if not first_txn:
            return []
        return list(first_txn.raw_data.keys())


class LocationClassificationSerializer(serializers.ModelSerializer):
    class Meta:
        model = LocationClassification
        fields = '__all__'


class LocationSubClassificationSerializer(serializers.ModelSerializer):
    # Read: full nested LocationClassification object
    location_classification = LocationClassificationSerializer(read_only=True)
    # Write: accept an integer ID
    location_classification_id = serializers.PrimaryKeyRelatedField(
        queryset=LocationClassification.objects.all(),
        source='location_classification',
        write_only=True,
    )

    class Meta:
        model = LocationSubClassification
        fields = [
            'id', 'location_classification', 'location_classification_id',
            'name', 'created_at', 'updated_at',
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
