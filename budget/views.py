from rest_framework import viewsets, status
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.decorators import action
from rest_framework.parsers import MultiPartParser, FormParser, JSONParser
from django.core.paginator import Paginator
from django.shortcuts import get_object_or_404
from drf_spectacular.utils import extend_schema, OpenApiParameter, inline_serializer
from rest_framework import fields as drf_fields
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
from .serializers import (
    AccountSerializer,
    FileUploadSerializer,
    TransactionSerializer,
    LocationClassificationSerializer,
    LocationSubClassificationSerializer,
    TimeClassificationSerializer,
    PersonClassificationSerializer,
    StatementSerializer,
)
from .csv_utils import parse_csv, apply_schema_to_transaction

TRANSACTIONS_DEFAULT_PAGE_SIZE = 100

ALLOWED_SORT_FIELDS = {
    "id", "account__name", "transaction_date",
    "description", "amount", "category", "subcategory",
}


class AccountViewSet(viewsets.ModelViewSet):
    queryset = Account.objects.order_by('id')
    serializer_class = AccountSerializer


class FileUploadViewSet(viewsets.ModelViewSet):
    queryset = FileUpload.objects.order_by('id')
    serializer_class = FileUploadSerializer

    def get_parsers(self):
        if getattr(self, "action", None) == "create":
            return [MultiPartParser(), FormParser()]
        return super().get_parsers()

    def create(self, request):
        """
        POST /api/v1/file-uploads/
        Accepts multipart/form-data with:
          - account_id: account ID (required)
          - file: a CSV file (optional)
        When a file is provided, parses it and creates one Transaction per row.
        Applies the account schema immediately if one exists.
        """
        account_id = request.data.get("account_id")
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
    

class LocationClassificationViewSet(viewsets.ModelViewSet):
    queryset = LocationClassification.objects.order_by('id')
    serializer_class = LocationClassificationSerializer


class LocationSubClassificationViewSet(viewsets.ModelViewSet):
    queryset = LocationSubClassification.objects.order_by('id')
    serializer_class = LocationSubClassificationSerializer


class TimeClassificationViewSet(viewsets.ModelViewSet):
    queryset = TimeClassification.objects.order_by('id')
    serializer_class = TimeClassificationSerializer


class PersonClassificationViewSet(viewsets.ModelViewSet):
    queryset = PersonClassification.objects.order_by('id')
    serializer_class = PersonClassificationSerializer


class TransactionListView(APIView):
    """Handles GET /transactions/ and POST /transactions/"""
    serializer_class = TransactionSerializer

    @extend_schema(
        operation_id="transactions_list",
        parameters=[
            OpenApiParameter(name="page", type=int, location="query", required=False, description="Page number (1-indexed)"),
            OpenApiParameter(name="page_size", type=int, location="query", required=False, description="Items per page"),
            OpenApiParameter(name="account", type=int, location="query", required=False, description="Filter by account ID"),
            OpenApiParameter(name="file_upload", type=int, location="query", required=False, description="Filter by file upload ID"),
            OpenApiParameter(name="transaction_date_from", type=str, location="query", required=False, description="Filter transactions on or after this date (ISO 8601, e.g. 2025-01-01)"),
            OpenApiParameter(name="transaction_date_to", type=str, location="query", required=False, description="Filter transactions on or before this date (ISO 8601, e.g. 2025-12-31)"),
            OpenApiParameter(name="description", type=str, location="query", required=False, description="Filter by description (case-insensitive substring match)"),
            OpenApiParameter(name="sort_by", type=str, location="query", required=False, description="Sort field, optionally prefixed with '-' for descending (e.g. '-amount'). Allowed values: id, account__name, transaction_date, description, amount, category, subcategory. Defaults to -created_at."),
            OpenApiParameter(name="location_classification", type=int, location="query", required=False, description="Filter by location classification ID"),
            OpenApiParameter(name="location_subclassification", type=int, location="query", required=False, description="Filter by location subclassification ID"),
            OpenApiParameter(name="time_classification", type=int, location="query", required=False, description="Filter by time classification ID"),
            OpenApiParameter(name="person_classification", type=int, location="query", required=False, description="Filter by person classification ID"),
        ],
        responses=inline_serializer(
            name='PaginatedTransactionList',
            fields={
                'count': drf_fields.IntegerField(),
                'total_pages': drf_fields.IntegerField(),
                'page': drf_fields.IntegerField(),
                'page_size': drf_fields.IntegerField(),
                'results': TransactionSerializer(many=True),
            },
        ),
    )
    def get(self, request):
        transactions = Transaction.objects.all()

        account_id = request.query_params.get("account")
        if account_id:
            transactions = transactions.filter(account_id=int(account_id))

        file_upload_id = request.query_params.get("file_upload")
        if file_upload_id:
            transactions = transactions.filter(file_upload_id=int(file_upload_id))

        transaction_date_from = request.query_params.get("transaction_date_from")
        if transaction_date_from:
            transactions = transactions.filter(transaction_date__date__gte=transaction_date_from)

        transaction_date_to = request.query_params.get("transaction_date_to")
        if transaction_date_to:
            transactions = transactions.filter(transaction_date__date__lte=transaction_date_to)

        description = request.query_params.get("description")
        if description:
            transactions = transactions.filter(description__icontains=description)

        location_classification_id = request.query_params.get("location_classification")
        if location_classification_id:
            transactions = transactions.filter(location_classification_id=int(location_classification_id))

        location_subclassification_id = request.query_params.get("location_subclassification")
        if location_subclassification_id:
            transactions = transactions.filter(location_subclassification_id=int(location_subclassification_id))

        time_classification_id = request.query_params.get("time_classification")
        if time_classification_id:
            transactions = transactions.filter(time_classification_id=int(time_classification_id))

        person_classification_id = request.query_params.get("person_classification")
        if person_classification_id:
            transactions = transactions.filter(person_classification_id=int(person_classification_id))

        sort_by = request.query_params.get("sort_by", "-created_at")
        direction = ""
        field = sort_by
        if sort_by.startswith("-"):
            direction = "-"
            field = sort_by[1:]
        if field in ALLOWED_SORT_FIELDS:
            transactions = transactions.order_by(f"{direction}{field}")
        else:
            transactions = transactions.order_by("-created_at")

        page_size = int(request.query_params.get("page_size", TRANSACTIONS_DEFAULT_PAGE_SIZE))
        page_number = int(request.query_params.get("page", 1))

        paginator = Paginator(transactions, page_size)
        page_obj = paginator.get_page(page_number)

        serializer = TransactionSerializer(page_obj.object_list, many=True)

        return Response({
            "count": paginator.count,
            "total_pages": paginator.num_pages,
            "page": page_obj.number,
            "page_size": page_size,
            "results": serializer.data,
        })

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


class StatementViewSet(viewsets.ModelViewSet):
    queryset = Statement.objects.order_by('id')
    serializer_class = StatementSerializer
