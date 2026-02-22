from rest_framework import serializers
from .models import Account, FileUpload, Transaction

class AccountSerializer(serializers.ModelSerializer):
    class Meta:
        model = Account
        fields = '__all__'

class FileUploadSerializer(serializers.ModelSerializer):
    headers = serializers.SerializerMethodField()

    class Meta:
        model = FileUpload
        fields = '__all__'

    def get_headers(self, obj):
        first_txn = obj.transactions.first()
        if not first_txn:
            return []
        return list(first_txn.raw_data.keys())

    def to_representation(self, instance):
        data = super().to_representation(instance)
        data['account'] = AccountSerializer(instance.account).data
        return data

class TransactionSerializer(serializers.ModelSerializer):
    class Meta:
        model = Transaction
        fields = '__all__'
