from rest_framework import viewsets, status
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.decorators import action
from rest_framework.parsers import MultiPartParser, FormParser, JSONParser
from django.shortcuts import get_object_or_404
from drf_spectacular.utils import extend_schema
from .models import Account, FileUpload, Transaction
from .serializers import AccountSerializer, FileUploadSerializer, TransactionSerializer
from .csv_utils import parse_csv, apply_schema_to_transaction


class AccountViewSet(viewsets.ModelViewSet):
    queryset = Account.objects.all()
    serializer_class = AccountSerializer


class FileUploadViewSet(viewsets.ModelViewSet):
    queryset = FileUpload.objects.all()
    serializer_class = FileUploadSerializer

    def get_parsers(self):
        if getattr(self, "action", None) == "create":
            return [MultiPartParser(), FormParser()]
        return super().get_parsers()

    def create(self, request):
        """
        POST /api/v1/file-uploads/
        Accepts multipart/form-data with:
          - account: account ID (required)
          - file: a CSV file (optional)
        When a file is provided, parses it and creates one Transaction per row.
        Applies the account schema immediately if one exists.
        """
        account_id = request.data.get("account")
        if not account_id:
            return Response({"detail": "No account provided."}, status=status.HTTP_400_BAD_REQUEST)

        account = get_object_or_404(Account, pk=account_id)
        file = request.FILES.get("file")

        if file:
            try:
                headers, rows = parse_csv(file)
            except Exception as exc:
                return Response({"detail": f"Failed to parse CSV: {exc}"}, status=status.HTTP_400_BAD_REQUEST)

            has_schema = bool(account.file_upload_schema)
            initial_status = FileUpload.STATUS_COMPLETED if has_schema else FileUpload.STATUS_PENDING

            file_upload = FileUpload.objects.create(
                account=account,
                filename=file.name,
                transaction_count=len(rows),
                status=initial_status,
            )

            transactions = []
            for row in rows:
                transactions.append(Transaction(
                    account=account,
                    file_upload=file_upload,
                    raw_data=dict(row),
                ))
            Transaction.objects.bulk_create(transactions)

            if has_schema:
                for txn in file_upload.transactions.all():
                    apply_schema_to_transaction(txn, account.file_upload_schema)
        else:
            file_upload = FileUpload.objects.create(
                account=account,
                filename=request.data.get("filename", ""),
                status=FileUpload.STATUS_PENDING,
            )

        serializer = FileUploadSerializer(file_upload)
        return Response(serializer.data, status=status.HTTP_201_CREATED)

    @extend_schema(operation_id="file_uploads_process")
    @action(
        detail=True,
        methods=["post"],
        url_path="process",
        parser_classes=[JSONParser],
    )
    def process(self, request, pk=None):
        """
        POST /api/v1/file-uploads/{id}/process/
        Re-processes all transactions for this FileUpload using the account schema.
        """
        file_upload = get_object_or_404(FileUpload, pk=pk)
        account = file_upload.account
        schema = account.file_upload_schema

        if not schema:
            return Response(
                {"detail": "Account has no file_upload_schema. Configure it first."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        file_upload.status = FileUpload.STATUS_PROCESSING
        file_upload.save(update_fields=["status"])

        errors = []
        for txn in file_upload.transactions.all():
            try:
                apply_schema_to_transaction(txn, schema)
            except Exception as exc:
                errors.append(str(exc))

        if errors:
            file_upload.status = FileUpload.STATUS_FAILED
            file_upload.errors = "\n".join(errors)
        else:
            file_upload.status = FileUpload.STATUS_COMPLETED
            file_upload.errors = None

        file_upload.save(update_fields=["status", "errors"])

        serializer = FileUploadSerializer(file_upload)
        return Response(serializer.data)


class TransactionListView(APIView):
    """Handles GET /transactions/ and POST /transactions/"""
    serializer_class = TransactionSerializer

    @extend_schema(operation_id="transactions_list")
    def get(self, request):
        transactions = Transaction.objects.all()
        serializer = TransactionSerializer(transactions, many=True)
        return Response(serializer.data)

    @extend_schema(operation_id="transactions_create")
    def post(self, request):
        serializer = TransactionSerializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class TransactionDetailView(APIView):
    """Handles GET /transactions/<id>/, PATCH /transactions/<id>/, DELETE /transactions/<id>/"""
    serializer_class = TransactionSerializer

    @extend_schema(operation_id="transactions_retrieve")
    def get(self, request, pk):
        transaction = get_object_or_404(Transaction, pk=pk)
        serializer = TransactionSerializer(transaction)
        return Response(serializer.data)

    @extend_schema(operation_id="transactions_partial_update")
    def patch(self, request, pk):
        transaction = get_object_or_404(Transaction, pk=pk)
        serializer = TransactionSerializer(transaction, data=request.data, partial=True)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    @extend_schema(operation_id="transactions_destroy")
    def delete(self, request, pk):
        transaction = get_object_or_404(Transaction, pk=pk)
        transaction.delete()
        return Response(status=status.HTTP_204_NO_CONTENT)
