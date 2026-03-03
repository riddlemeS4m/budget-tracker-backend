import csv
import io
from collections import defaultdict
from decimal import Decimal
from django.db.models import Count, Sum, F as models_F, OuterRef, Subquery
from django.db.models.functions import TruncMonth
from rest_framework import viewsets, status
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.decorators import action
from rest_framework.parsers import MultiPartParser, FormParser, JSONParser
from django.core.paginator import Paginator
from django.http import StreamingHttpResponse
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
    TransactionBatchUpdateSerializer,
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
    queryset = LocationClassification.objects.annotate(
        transaction_count=Count('transactions', distinct=True),
        subcategory_count=Count('location_subclassifications', distinct=True),
    ).order_by('name')
    serializer_class = LocationClassificationSerializer


class LocationSubClassificationViewSet(viewsets.ModelViewSet):
    queryset = LocationSubClassification.objects.none()
    serializer_class = LocationSubClassificationSerializer

    def get_queryset(self):
        queryset = LocationSubClassification.objects.annotate(
            transaction_count=Count('transactions', distinct=True),
        ).order_by('name')

        location_classification_id = self.request.query_params.get('location_classification')
        if location_classification_id:
            queryset = queryset.filter(location_classification_id=int(location_classification_id))

        type_ = self.request.query_params.get('type')
        if type_:
            queryset = queryset.filter(location_classification__type=type_)

        name = self.request.query_params.get('name')
        if name:
            queryset = queryset.filter(name__icontains=name)

        return queryset

    @extend_schema(
        parameters=[
            OpenApiParameter(name='location_classification', type=int, location='query', required=False, description='Filter by location classification ID'),
            OpenApiParameter(name='type', type=str, location='query', required=False, description='Filter by location classification type (income, expense, transfer)'),
            OpenApiParameter(name='name', type=str, location='query', required=False, description='Filter by name (case-insensitive substring match)'),
        ]
    )
    def list(self, request, *args, **kwargs):
        return super().list(request, *args, **kwargs)


class TimeClassificationViewSet(viewsets.ModelViewSet):
    queryset = TimeClassification.objects.order_by('name')
    serializer_class = TimeClassificationSerializer


class PersonClassificationViewSet(viewsets.ModelViewSet):
    queryset = PersonClassification.objects.order_by('name')
    serializer_class = PersonClassificationSerializer


def _apply_transaction_filters(queryset, query_params):
    """Apply filter and sort query params to a Transaction queryset."""
    account_id = query_params.get("account")
    if account_id:
        queryset = queryset.filter(account_id=int(account_id))

    file_upload_id = query_params.get("file_upload")
    if file_upload_id:
        queryset = queryset.filter(file_upload_id=int(file_upload_id))

    transaction_date_from = query_params.get("transaction_date_from")
    if transaction_date_from:
        queryset = queryset.filter(transaction_date__date__gte=transaction_date_from)

    transaction_date_to = query_params.get("transaction_date_to")
    if transaction_date_to:
        queryset = queryset.filter(transaction_date__date__lte=transaction_date_to)

    description = query_params.get("description")
    if description:
        queryset = queryset.filter(description__icontains=description)

    location_classification_id = query_params.get("location_classification")
    if location_classification_id:
        queryset = queryset.filter(location_classification_id=int(location_classification_id))

    location_classification_null = query_params.get("location_classification_null")
    if location_classification_null == "true":
        queryset = queryset.filter(location_classification__isnull=True)

    location_subclassification_id = query_params.get("location_subclassification")
    if location_subclassification_id:
        queryset = queryset.filter(location_subclassification_id=int(location_subclassification_id))

    time_classification_id = query_params.get("time_classification")
    if time_classification_id:
        queryset = queryset.filter(time_classification_id=int(time_classification_id))

    person_classification_id = query_params.get("person_classification")
    if person_classification_id:
        queryset = queryset.filter(person_classification_id=int(person_classification_id))

    account_type = query_params.get("account_type")
    if account_type:
        queryset = queryset.filter(account__type=account_type)

    excluded_account_type = query_params.get("excluded_account_type")
    if excluded_account_type:
        queryset = queryset.exclude(account__type=excluded_account_type)

    location_classification_type = query_params.get("location_classification_type")
    if location_classification_type:
        queryset = queryset.filter(location_classification__type=location_classification_type)

    sort_by = query_params.get("sort_by", "-created_at")
    direction = ""
    field = sort_by
    if sort_by.startswith("-"):
        direction = "-"
        field = sort_by[1:]
    if field in ALLOWED_SORT_FIELDS:
        queryset = queryset.order_by(f"{direction}{field}")
    else:
        queryset = queryset.order_by("-created_at")

    return queryset


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
            OpenApiParameter(name="location_classification_null", type=str, location="query", required=False, description="Pass 'true' to filter only transactions with no location classification"),
            OpenApiParameter(name="location_subclassification", type=int, location="query", required=False, description="Filter by location subclassification ID"),
            OpenApiParameter(name="time_classification", type=int, location="query", required=False, description="Filter by time classification ID"),
            OpenApiParameter(name="person_classification", type=int, location="query", required=False, description="Filter by person classification ID"),
            OpenApiParameter(name="account_type", type=str, location="query", required=False, description="Filter by account type (e.g. payroll, checking, savings)"),
            OpenApiParameter(name="excluded_account_type", type=str, location="query", required=False, description="Exclude accounts of the given type (e.g. payroll)"),
            OpenApiParameter(name="location_classification_type", type=str, location="query", required=False, description="Filter by location classification type (income, expense, transfer)"),
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
        transactions = _apply_transaction_filters(Transaction.objects.all(), request.query_params)

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


class TransactionBatchUpdateView(APIView):
    """Handles POST /transactions/batch-update/ — bulk-updates classification fields on selected transactions."""

    @extend_schema(
        operation_id="transactions_batch_update",
        request=TransactionBatchUpdateSerializer,
        responses=inline_serializer(
            name='TransactionBatchUpdateResponse',
            fields={'updated': drf_fields.IntegerField()},
        ),
        description=(
            "Update classification fields on multiple transactions at once. "
            "Only fields present in the request body are modified; omitted fields are left unchanged. "
            "Pass null to clear a classification."
        ),
    )
    def post(self, request):
        serializer = TransactionBatchUpdateSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

        data = serializer.validated_data
        ids = data.pop('ids')

        update_kwargs = {}
        for field in ('location_classification', 'location_subclassification',
                      'time_classification', 'person_classification'):
            if field in data:
                update_kwargs[field] = data[field]

        if not update_kwargs:
            return Response({'updated': 0})

        updated_count = Transaction.objects.filter(id__in=ids).update(**update_kwargs)
        return Response({'updated': updated_count})


class TransactionExportView(APIView):
    """Handles GET /transactions/export/ — streams all matching transactions as a CSV file."""

    @extend_schema(
        operation_id="transactions_export",
        parameters=[
            OpenApiParameter(name="account", type=int, location="query", required=False, description="Filter by account ID"),
            OpenApiParameter(name="file_upload", type=int, location="query", required=False, description="Filter by file upload ID"),
            OpenApiParameter(name="transaction_date_from", type=str, location="query", required=False, description="Filter transactions on or after this date (ISO 8601)"),
            OpenApiParameter(name="transaction_date_to", type=str, location="query", required=False, description="Filter transactions on or before this date (ISO 8601)"),
            OpenApiParameter(name="description", type=str, location="query", required=False, description="Filter by description (case-insensitive substring match)"),
            OpenApiParameter(name="sort_by", type=str, location="query", required=False, description="Sort field, optionally prefixed with '-' for descending"),
            OpenApiParameter(name="location_classification", type=int, location="query", required=False, description="Filter by location classification ID"),
            OpenApiParameter(name="location_subclassification", type=int, location="query", required=False, description="Filter by location subclassification ID"),
            OpenApiParameter(name="time_classification", type=int, location="query", required=False, description="Filter by time classification ID"),
            OpenApiParameter(name="person_classification", type=int, location="query", required=False, description="Filter by person classification ID"),
        ],
        responses={200: None},
    )
    def get(self, request):
        transactions = _apply_transaction_filters(
            Transaction.objects.select_related(
                "account",
                "location_classification",
                "location_subclassification",
                "time_classification",
                "person_classification",
            ),
            request.query_params,
        )

        def _name(obj):
            return obj.name if obj is not None else ""

        def stream_csv():
            buffer = io.StringIO()
            writer = csv.writer(buffer)
            writer.writerow([
                "ID", "Account", "Transaction Date", "Posted Date",
                "Description", "Description 2", "Category", "Subcategory",
                "Amount", "Location Classification", "Location Subclassification",
                "Time Classification", "Person Classification",
            ])
            yield buffer.getvalue()

            for tx in transactions.iterator():
                buffer = io.StringIO()
                writer = csv.writer(buffer)
                writer.writerow([
                    tx.id,
                    tx.account.name if tx.account else "",
                    tx.transaction_date.isoformat() if tx.transaction_date else "",
                    tx.posted_date.isoformat() if tx.posted_date else "",
                    tx.description or "",
                    tx.description_2 or "",
                    tx.category or "",
                    tx.subcategory or "",
                    tx.amount if tx.amount is not None else "",
                    _name(tx.location_classification),
                    _name(tx.location_subclassification),
                    _name(tx.time_classification),
                    _name(tx.person_classification),
                ])
                yield buffer.getvalue()

        response = StreamingHttpResponse(stream_csv(), content_type="text/csv")
        response["Content-Disposition"] = 'attachment; filename="transactions.csv"'
        return response


STATEMENT_SORT_FIELDS = {
    "id", "account__name", "period_start", "period_end",
    "opening_balance", "closing_balance",
}


class StatementViewSet(viewsets.ModelViewSet):
    queryset = Statement.objects.none()
    serializer_class = StatementSerializer

    def get_queryset(self):
        queryset = Statement.objects.select_related('account')

        account_id = self.request.query_params.get('account')
        if account_id:
            queryset = queryset.filter(account_id=int(account_id))

        date_from = self.request.query_params.get('date_from')
        if date_from:
            queryset = queryset.filter(period_end__gte=date_from)

        date_to = self.request.query_params.get('date_to')
        if date_to:
            queryset = queryset.filter(period_end__lte=date_to)

        sort_by = self.request.query_params.get('sort_by', 'id')
        direction = ''
        field = sort_by
        if sort_by.startswith('-'):
            direction = '-'
            field = sort_by[1:]
        if field in STATEMENT_SORT_FIELDS:
            queryset = queryset.order_by(f'{direction}{field}')
        else:
            queryset = queryset.order_by('id')

        return queryset

    @extend_schema(
        parameters=[
            OpenApiParameter(name='account', type=int, location='query', required=False,
                             description='Filter by account ID'),
            OpenApiParameter(name='date_from', type=str, location='query', required=False,
                             description='Filter statements with period_end on or after this date (ISO 8601, e.g. 2025-01-01)'),
            OpenApiParameter(name='date_to', type=str, location='query', required=False,
                             description='Filter statements with period_end on or before this date (ISO 8601, e.g. 2025-12-31)'),
            OpenApiParameter(name='sort_by', type=str, location='query', required=False,
                             description='Sort field. Prefix with "-" for descending. Allowed: id, account__name, period_start, period_end, opening_balance, closing_balance.'),
        ]
    )
    def list(self, request, *args, **kwargs):
        return super().list(request, *args, **kwargs)


# ---------------------------------------------------------------------------
# Reports
# ---------------------------------------------------------------------------

def _build_summary_sections(rows):
    """
    Given aggregated rows of the form:
        {
            'cls_id': int | None,
            'cls_name': str | None,
            'cls_type': str | None,
            'sub_id': int | None,
            'sub_name': str | None,
            'total': Decimal,
        }
    assemble the two-level hierarchy expected by both report modes.
    Returns (sections, total_revenues, total_expenses).
    """
    tree = {}  # type -> {cat_key -> {sub_key -> total}}
    cat_meta = {}   # cat_key -> {id, name, type}
    sub_meta = {}   # sub_key -> {id, name}

    for row in rows:
        cls_type = row['cls_type'] or 'expense'
        cat_key = (row['cls_id'], row['cls_name'] or 'Unclassified')
        sub_key = (row['sub_id'], row['sub_name'] or 'Uncategorized')
        total = row['total'] or Decimal('0')

        if cls_type not in tree:
            tree[cls_type] = {}
        if cat_key not in tree[cls_type]:
            tree[cls_type][cat_key] = {}
            cat_meta[cat_key] = {'id': row['cls_id'], 'name': cat_key[1], 'type': cls_type}
        tree[cls_type][cat_key][sub_key] = tree[cls_type][cat_key].get(sub_key, Decimal('0')) + total
        sub_meta[sub_key] = {'id': row['sub_id'], 'name': sub_key[1]}

    sections = []
    section_defs = [
        ('income', 'Revenues'),
        ('expense', 'Expenses'),
    ]
    total_revenues = Decimal('0')
    total_expenses = Decimal('0')

    for cls_type, label in section_defs:
        categories = []
        section_total = Decimal('0')

        for cat_key, subs in sorted(tree.get(cls_type, {}).items(), key=lambda x: (x[0][0] is None, x[0][0])):
            cat_total = Decimal('0')
            subcategories = []
            for sub_key, total in sorted(subs.items(), key=lambda x: (x[0][0] is None, x[0][0])):
                subcategories.append({
                    'id': sub_meta[sub_key]['id'],
                    'name': sub_meta[sub_key]['name'],
                    'total': str(total),
                })
                cat_total += total
            categories.append({
                'id': cat_meta[cat_key]['id'],
                'name': cat_meta[cat_key]['name'],
                'subcategories': subcategories,
                'total': str(cat_total),
            })
            section_total += cat_total

        sections.append({
            'type': cls_type,
            'label': label,
            'categories': categories,
            'total': str(section_total),
        })

        if cls_type == 'income':
            total_revenues = section_total
        elif cls_type == 'expense':
            total_expenses = section_total

    return sections, total_revenues, total_expenses


class CashFlowStatementViewSet(viewsets.ViewSet):
    """
    Reports viewset for cash flow statement.
    Provides two actions:
      - summary: single-period totals (for Exploration)
      - monthly: 12-month breakdown + YTD (for Audit)
    """

    @extend_schema(
        operation_id='cash_flow_statement_summary',
        parameters=[
            OpenApiParameter(name='date_from', type=str, location='query', required=False,
                             description='Start date (ISO 8601, e.g. 2025-01-01)'),
            OpenApiParameter(name='date_to', type=str, location='query', required=False,
                             description='End date (ISO 8601, e.g. 2025-12-31)'),
            OpenApiParameter(name='account', type=int, location='query', required=False,
                             description='Filter by account ID'),
        ],
        responses={200: inline_serializer(
            name='CashFlowStatementSummary',
            fields={
                'date_from': drf_fields.CharField(allow_null=True),
                'date_to': drf_fields.CharField(allow_null=True),
                'sections': drf_fields.ListField(child=drf_fields.DictField()),
                'total_revenues': drf_fields.CharField(),
                'total_expenses': drf_fields.CharField(),
                'net_income': drf_fields.CharField(),
            },
        )},
    )
    @action(detail=False, methods=['get'], url_path='summary')
    def summary(self, request):
        """GET /api/v1/reports/cash-flow-statement/summary/"""
        date_from = request.query_params.get('date_from')
        date_to = request.query_params.get('date_to')
        account_id = request.query_params.get('account')

        qs = Transaction.objects.filter(
            location_classification__type__in=['income', 'expense'],
        )
        if date_from:
            qs = qs.filter(transaction_date__date__gte=date_from)
        if date_to:
            qs = qs.filter(transaction_date__date__lte=date_to)
        if account_id:
            qs = qs.filter(account_id=int(account_id))

        agg_fields = dict(
            cls_id=models_F('location_classification__id'),
            cls_name=models_F('location_classification__name'),
            cls_type=models_F('location_classification__type'),
            sub_id=models_F('location_subclassification__id'),
            sub_name=models_F('location_subclassification__name'),
        )
        rows = list(qs.values(**agg_fields).annotate(total=Sum('amount')))

        if account_id:
            transfer_qs = Transaction.objects.filter(
                location_classification__type=LocationClassification.TYPE_TRANSFER,
                account_id=int(account_id),
            )
            if date_from:
                transfer_qs = transfer_qs.filter(transaction_date__date__gte=date_from)
            if date_to:
                transfer_qs = transfer_qs.filter(transaction_date__date__lte=date_to)

            transfer_agg = dict(
                cls_id=models_F('location_classification__id'),
                cls_name=models_F('location_classification__name'),
                sub_id=models_F('location_subclassification__id'),
                sub_name=models_F('location_subclassification__name'),
            )
            for row in transfer_qs.filter(amount__gt=0).values(**transfer_agg).annotate(total=Sum('amount')):
                row['cls_type'] = 'income'
                row['cls_name'] = 'Transfers In'
                row['cls_id'] = None
                rows.append(row)
            for row in transfer_qs.filter(amount__lt=0).values(**transfer_agg).annotate(total=Sum('amount')):
                row['cls_type'] = 'expense'
                row['cls_name'] = 'Transfers Out'
                row['cls_id'] = None
                rows.append(row)

        sections, total_revenues, total_expenses = _build_summary_sections(rows)

        return Response({
            'date_from': date_from,
            'date_to': date_to,
            'sections': sections,
            'total_revenues': str(total_revenues),
            'total_expenses': str(total_expenses),
            'net_income': str(total_revenues + total_expenses),
        })

    @extend_schema(
        operation_id='cash_flow_statement_monthly',
        parameters=[
            OpenApiParameter(name='year', type=int, location='query', required=True,
                             description='The calendar year (e.g. 2025)'),
        ],
        responses={200: inline_serializer(
            name='CashFlowStatementMonthly',
            fields={
                'year': drf_fields.IntegerField(),
                'months': drf_fields.ListField(child=drf_fields.CharField()),
                'sections': drf_fields.ListField(child=drf_fields.DictField()),
                'total_revenues': drf_fields.DictField(),
                'total_expenses': drf_fields.DictField(),
                'net_income': drf_fields.DictField(),
            },
        )},
    )
    @action(detail=False, methods=['get'], url_path='monthly')
    def monthly(self, request):
        """GET /api/v1/reports/cash-flow-statement/monthly/?year=2025"""
        year_param = request.query_params.get('year')
        if not year_param:
            return Response({'detail': 'year parameter is required.'}, status=status.HTTP_400_BAD_REQUEST)

        try:
            year = int(year_param)
        except ValueError:
            return Response({'detail': 'year must be an integer.'}, status=status.HTTP_400_BAD_REQUEST)

        MONTH_LABELS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                        'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

        qs = Transaction.objects.filter(
            location_classification__type__in=['income', 'expense'],
            transaction_date__year=year,
        )

        raw_rows = qs.annotate(
            month=TruncMonth('transaction_date'),
        ).values(
            'month',
            cls_id=models_F('location_classification__id'),
            cls_name=models_F('location_classification__name'),
            cls_type=models_F('location_classification__type'),
            sub_id=models_F('location_subclassification__id'),
            sub_name=models_F('location_subclassification__name'),
        ).annotate(total=Sum('amount'))

        # Pivot: (cls_type, cat_key, sub_key) -> month_index -> total
        pivot = defaultdict(lambda: defaultdict(Decimal))
        cat_meta = {}
        sub_meta = {}

        for row in raw_rows:
            cls_type = row['cls_type'] or 'expense'
            cat_key = (row['cls_id'], row['cls_name'] or 'Unclassified')
            sub_key = (row['sub_id'], row['sub_name'] or 'Uncategorized')
            month_idx = row['month'].month  # 1-12
            total = row['total'] or Decimal('0')

            pivot_key = (cls_type, cat_key, sub_key)
            pivot[pivot_key][month_idx] += total

            cat_meta[cat_key] = {'id': row['cls_id'], 'name': cat_key[1], 'type': cls_type}
            sub_meta[sub_key] = {'id': row['sub_id'], 'name': sub_key[1]}

        def zero_months():
            return {m: Decimal('0') for m in range(1, 13)}

        def months_to_response(month_dict):
            return {str(m): str(month_dict.get(m, Decimal('0'))) for m in range(1, 13)}

        def ytd(month_dict):
            return str(sum(month_dict.values(), Decimal('0')))

        # Build sections
        sections = []
        section_defs = [('income', 'Revenues'), ('expense', 'Expenses')]
        total_rev_months = zero_months()
        total_exp_months = zero_months()

        for cls_type, label in section_defs:
            # Collect categories for this section
            cats_map = defaultdict(lambda: {'subs': {}, 'months': zero_months()})

            for (pt, cat_key, sub_key), month_totals in pivot.items():
                if pt != cls_type:
                    continue
                for m, total in month_totals.items():
                    cats_map[cat_key]['months'][m] += total
                    cats_map[cat_key]['subs'].setdefault(sub_key, zero_months())
                    cats_map[cat_key]['subs'][sub_key][m] += total

            section_months = zero_months()
            categories = []
            for cat_key, cat_data in sorted(cats_map.items(), key=lambda x: (x[0][0] is None, x[0][0])):
                sub_list = []
                for sub_key, sub_months in sorted(cat_data['subs'].items(), key=lambda x: (x[0][0] is None, x[0][0])):
                    sub_list.append({
                        'id': sub_meta[sub_key]['id'],
                        'name': sub_meta[sub_key]['name'],
                        'months': months_to_response(sub_months),
                        'ytd': ytd(sub_months),
                    })
                cat_months = cat_data['months']
                categories.append({
                    'id': cat_meta[cat_key]['id'],
                    'name': cat_meta[cat_key]['name'],
                    'subcategories': sub_list,
                    'months': months_to_response(cat_months),
                    'ytd': ytd(cat_months),
                })
                for m, v in cat_months.items():
                    section_months[m] += v

            sections.append({
                'type': cls_type,
                'label': label,
                'categories': categories,
                'months': months_to_response(section_months),
                'ytd': ytd(section_months),
            })

            if cls_type == 'income':
                for m, v in section_months.items():
                    total_rev_months[m] += v
            elif cls_type == 'expense':
                for m, v in section_months.items():
                    total_exp_months[m] += v

        net_months = {m: total_rev_months[m] + total_exp_months[m] for m in range(1, 13)}

        return Response({
            'year': year,
            'months': MONTH_LABELS,
            'sections': sections,
            'total_revenues': {'months': months_to_response(total_rev_months), 'ytd': ytd(total_rev_months)},
            'total_expenses': {'months': months_to_response(total_exp_months), 'ytd': ytd(total_exp_months)},
            'net_income': {'months': months_to_response(net_months), 'ytd': ytd(net_months)},
        })


class StatementReconciliationViewSet(viewsets.ViewSet):
    """
    Audit report: for each statement, compare the expected balance change
    (closing_balance - opening_balance) against the sum of matching transactions.
    Surfaces import gaps or sign convention issues.
    """

    @extend_schema(
        operation_id='statement_reconciliation_list',
        parameters=[
            OpenApiParameter(name='account', type=int, location='query', required=False,
                             description='Filter by account ID'),
            OpenApiParameter(name='year', type=int, location='query', required=False,
                             description='Filter by year of period_end'),
        ],
        responses={200: inline_serializer(
            name='StatementReconciliation',
            fields={
                'rows': drf_fields.ListField(child=drf_fields.DictField()),
                'summary': drf_fields.DictField(),
            },
        )},
    )
    def list(self, request):
        """GET /api/v1/reports/statement-reconciliation/"""
        account_id = request.query_params.get('account')
        year_param = request.query_params.get('year')

        qs = Statement.objects.select_related('account').exclude(
            account__type=Account.TYPE_INVESTMENT
        ).order_by('account__name', 'period_end')

        if account_id:
            try:
                qs = qs.filter(account_id=int(account_id))
            except ValueError:
                return Response({'detail': 'account must be an integer.'}, status=status.HTTP_400_BAD_REQUEST)

        if year_param:
            try:
                qs = qs.filter(period_end__year=int(year_param))
            except ValueError:
                return Response({'detail': 'year must be an integer.'}, status=status.HTTP_400_BAD_REQUEST)

        # Annotate each statement with the sum and count of transactions that fall
        # within its period and belong to the same account.
        txn_sum_subquery = Subquery(
            Transaction.objects.filter(
                account=OuterRef('account'),
                transaction_date__date__gte=OuterRef('period_start'),
                transaction_date__date__lte=OuterRef('period_end'),
            ).values('account').annotate(s=Sum('amount')).values('s')[:1]
        )
        txn_count_subquery = Subquery(
            Transaction.objects.filter(
                account=OuterRef('account'),
                transaction_date__date__gte=OuterRef('period_start'),
                transaction_date__date__lte=OuterRef('period_end'),
            ).values('account').annotate(c=Count('id')).values('c')[:1]
        )

        qs = qs.annotate(
            txn_sum_annotated=txn_sum_subquery,
            txn_count_annotated=txn_count_subquery,
        )

        rows = []
        total_discrepancy = Decimal('0')
        reconciled_count = 0

        for stmt in qs:
            opening = stmt.opening_balance
            closing = stmt.closing_balance
            txn_sum = stmt.txn_sum_annotated if stmt.txn_sum_annotated is not None else Decimal('0')
            txn_count = stmt.txn_count_annotated if stmt.txn_count_annotated is not None else 0

            if opening is not None:
                # Credit card balances are liabilities: a rising balance means
                # more was spent (negative transactions), so negate the
                # statement delta to match the transaction sign convention.
                if stmt.account.type == Account.TYPE_CREDIT_CARD:
                    expected_change = -(closing - opening)
                else:
                    expected_change = closing - opening
                discrepancy = expected_change - txn_sum
                is_reconciled = discrepancy == Decimal('0')
                total_discrepancy += discrepancy
            else:
                expected_change = None
                discrepancy = None
                is_reconciled = False

            if is_reconciled:
                reconciled_count += 1

            rows.append({
                'statement_id': stmt.id,
                'account_id': stmt.account_id,
                'account_name': stmt.account.name,
                'period_start': str(stmt.period_start) if stmt.period_start else None,
                'period_end': str(stmt.period_end),
                'opening_balance': str(opening) if opening is not None else None,
                'closing_balance': str(closing),
                'expected_change': str(expected_change) if expected_change is not None else None,
                'transaction_sum': str(txn_sum),
                'transaction_count': txn_count,
                'discrepancy': str(discrepancy) if discrepancy is not None else None,
                'is_reconciled': is_reconciled,
            })

        total_statements = len(rows)
        unreconciled_count = total_statements - reconciled_count

        return Response({
            'rows': rows,
            'summary': {
                'total_statements': total_statements,
                'reconciled_count': reconciled_count,
                'unreconciled_count': unreconciled_count,
                'total_discrepancy': str(total_discrepancy),
            },
        })


class IncomeExpenseSummaryViewSet(viewsets.ViewSet):
    """
    Report that groups revenues and expenses by location classification,
    showing total, percent of section total, and transaction count per group,
    plus an uncategorized bucket for transactions with no location classification.
    """

    @extend_schema(
        operation_id='income_expense_summary',
        parameters=[
            OpenApiParameter(name='date_from', type=str, location='query', required=False,
                             description='Start date (ISO 8601, e.g. 2025-01-01)'),
            OpenApiParameter(name='date_to', type=str, location='query', required=False,
                             description='End date (ISO 8601, e.g. 2025-12-31)'),
            OpenApiParameter(name='account', type=int, location='query', required=False,
                             description='Filter by account ID'),
        ],
        responses={200: inline_serializer(
            name='IncomeExpenseSummary',
            fields={
                'date_from': drf_fields.CharField(allow_null=True),
                'date_to': drf_fields.CharField(allow_null=True),
                'sections': drf_fields.ListField(child=drf_fields.DictField()),
            },
        )},
    )
    @action(detail=False, methods=['get'], url_path='summary')
    def summary(self, request):
        """GET /api/v1/reports/income-expense-summary/summary/"""
        date_from = request.query_params.get('date_from')
        date_to = request.query_params.get('date_to')
        account_id = request.query_params.get('account')

        def _base_qs():
            qs = Transaction.objects.all()
            if date_from:
                qs = qs.filter(transaction_date__date__gte=date_from)
            if date_to:
                qs = qs.filter(transaction_date__date__lte=date_to)
            if account_id:
                qs = qs.filter(account_id=int(account_id))
            return qs

        # Classified transactions (income or expense)
        classified_rows = (
            _base_qs()
            .filter(location_classification__type__in=['income', 'expense'])
            .values(
                cls_id=models_F('location_classification__id'),
                cls_name=models_F('location_classification__name'),
                cls_type=models_F('location_classification__type'),
            )
            .annotate(total=Sum('amount'), transaction_count=Count('id'))
        )

        # Unclassified transactions: split by sign to assign to revenue vs expense
        unclassified_qs = _base_qs().filter(location_classification__isnull=True)
        unclassified_income = (
            unclassified_qs.filter(amount__gt=0)
            .aggregate(total=Sum('amount'), transaction_count=Count('id'))
        )
        unclassified_expense = (
            unclassified_qs.filter(amount__lt=0)
            .aggregate(total=Sum('amount'), transaction_count=Count('id'))
        )

        # Build tree: cls_type -> list of {id, name, total, count}
        tree = {'income': [], 'expense': []}
        for row in classified_rows:
            tree[row['cls_type']].append({
                'id': row['cls_id'],
                'name': row['cls_name'],
                'total': row['total'] or Decimal('0'),
                'transaction_count': row['transaction_count'],
            })

        def _build_section(cls_type, label, unclassified_agg):
            categories = tree[cls_type]
            # Sort by absolute value descending so largest groups appear first
            categories = sorted(categories, key=lambda c: abs(c['total']), reverse=True)

            cat_sum = sum(c['total'] for c in categories)
            unc_total = unclassified_agg['total'] or Decimal('0')
            unc_count = unclassified_agg['transaction_count'] or 0
            section_total = cat_sum + unc_total

            def _pct(amount):
                if section_total == 0:
                    return '0.00'
                return str(round(abs(amount) / abs(section_total) * 100, 2))

            category_out = [
                {
                    'id': c['id'],
                    'name': c['name'],
                    'total': str(c['total']),
                    'transaction_count': c['transaction_count'],
                    'percent': _pct(c['total']),
                }
                for c in categories
            ]

            return {
                'type': cls_type,
                'label': label,
                'total': str(section_total),
                'transaction_count': sum(c['transaction_count'] for c in categories) + unc_count,
                'categories': category_out,
                'uncategorized': {
                    'total': str(unc_total),
                    'transaction_count': unc_count,
                    'percent': _pct(unc_total),
                },
            }

        sections = [
            _build_section('income', 'Revenues', unclassified_income),
            _build_section('expense', 'Expenses', unclassified_expense),
        ]

        return Response({
            'date_from': date_from,
            'date_to': date_to,
            'sections': sections,
        })


class PayrollReportViewSet(viewsets.ViewSet):
    """
    Report that breaks down where the user's payroll goes each month:
    transfers to non-checking accounts, payroll deductions, and expenses
    from other accounts, all as dollar amounts and percentages of payroll.
    """

    @extend_schema(
        operation_id='payroll_summary',
        parameters=[
            OpenApiParameter(name='date_from', type=str, location='query', required=False,
                             description='Start date (ISO 8601, e.g. 2025-01-01)'),
            OpenApiParameter(name='date_to', type=str, location='query', required=False,
                             description='End date (ISO 8601, e.g. 2025-01-31)'),
        ],
        responses={200: inline_serializer(
            name='PayrollSummary',
            fields={
                'date_from': drf_fields.CharField(allow_null=True),
                'date_to': drf_fields.CharField(allow_null=True),
                'payroll_total': drf_fields.CharField(),
                'net_change': drf_fields.CharField(),
                'sections': drf_fields.ListField(child=drf_fields.DictField()),
            },
        )},
    )
    @action(detail=False, methods=['get'], url_path='summary')
    def summary(self, request):
        """GET /api/v1/reports/payroll/summary/"""
        date_from = request.query_params.get('date_from')
        date_to = request.query_params.get('date_to')

        payroll_account_ids = list(
            Account.objects.filter(type=Account.TYPE_PAYROLL).values_list('id', flat=True)
        )
        checking_account_ids = set(
            Account.objects.filter(type=Account.TYPE_CHECKING).values_list('id', flat=True)
        )

        def _date_filter(qs):
            if date_from:
                qs = qs.filter(transaction_date__date__gte=date_from)
            if date_to:
                qs = qs.filter(transaction_date__date__lte=date_to)
            return qs

        # ---------------------------------------------------------------
        # Payroll total: sum of positive transactions in payroll accounts
        # ---------------------------------------------------------------
        payroll_total_agg = _date_filter(
            Transaction.objects.filter(account_id__in=payroll_account_ids, amount__gt=0)
        ).aggregate(total=Sum('amount'))
        payroll_total = payroll_total_agg['total'] or Decimal('0')

        def _pct(amount):
            if payroll_total == 0:
                return '0.00'
            return str(round(abs(amount) / abs(payroll_total) * 100, 2))

        # ---------------------------------------------------------------
        # Section 1: Transfers from Payroll (excluding to checking)
        # Match outgoing transfer txns in payroll accounts to incoming
        # transfer txns in non-payroll accounts by amount + closest date.
        # ---------------------------------------------------------------
        payroll_transfers = list(
            _date_filter(
                Transaction.objects.filter(
                    account_id__in=payroll_account_ids,
                    location_classification__type=LocationClassification.TYPE_TRANSFER,
                    amount__lt=0,
                )
            ).select_related('account').order_by('transaction_date')
        )

        dest_candidates = list(
            _date_filter(
                Transaction.objects.filter(
                    location_classification__type=LocationClassification.TYPE_TRANSFER,
                    amount__gt=0,
                ).exclude(account_id__in=payroll_account_ids)
            ).select_related('account').order_by('transaction_date')
        )

        # For each payroll outgoing transfer, find the best-matching
        # destination candidate (same abs amount, closest date).
        transfer_by_account: dict[int, dict] = {}
        used_dest_ids: set[int] = set()

        for pt in payroll_transfers:
            target_amount = abs(pt.amount)
            best: Transaction | None = None
            best_delta = None
            for dc in dest_candidates:
                if dc.id in used_dest_ids:
                    continue
                if abs(dc.amount) != target_amount:
                    continue
                if pt.transaction_date is None or dc.transaction_date is None:
                    continue
                delta = abs((pt.transaction_date - dc.transaction_date).total_seconds())
                if best_delta is None or delta < best_delta:
                    best = dc
                    best_delta = delta

            if best is None:
                continue

            used_dest_ids.add(best.id)
            dest_acct_id = best.account_id
            # Exclude transfers to checking accounts
            if dest_acct_id in checking_account_ids:
                continue

            if dest_acct_id not in transfer_by_account:
                transfer_by_account[dest_acct_id] = {
                    'id': dest_acct_id,
                    'name': best.account.name,
                    'total': Decimal('0'),
                    'transaction_count': 0,
                }
            transfer_by_account[dest_acct_id]['total'] += pt.amount
            transfer_by_account[dest_acct_id]['transaction_count'] += 1

        transfer_categories = sorted(
            transfer_by_account.values(),
            key=lambda c: abs(c['total']),
            reverse=True,
        )
        transfer_section_total = sum(c['total'] for c in transfer_categories)
        transfer_section = {
            'type': 'transfers',
            'label': 'Transfers from Payroll',
            'total': str(transfer_section_total),
            'transaction_count': sum(c['transaction_count'] for c in transfer_categories),
            'categories': [
                {
                    'id': c['id'],
                    'name': c['name'],
                    'total': str(c['total']),
                    'transaction_count': c['transaction_count'],
                    'percent': _pct(c['total']),
                }
                for c in transfer_categories
            ],
            'uncategorized': {'total': '0.00', 'transaction_count': 0, 'percent': '0.00'},
        }

        # ---------------------------------------------------------------
        # Section 2: Payroll Deductions (expense txns in payroll accounts)
        # ---------------------------------------------------------------
        payroll_expense_rows = (
            _date_filter(
                Transaction.objects.filter(
                    account_id__in=payroll_account_ids,
                    location_classification__type=LocationClassification.TYPE_EXPENSE,
                )
            )
            .values(
                cls_id=models_F('location_classification__id'),
                cls_name=models_F('location_classification__name'),
            )
            .annotate(total=Sum('amount'), transaction_count=Count('id'))
        )
        payroll_expense_unclassified = (
            _date_filter(
                Transaction.objects.filter(
                    account_id__in=payroll_account_ids,
                    location_classification__isnull=True,
                    amount__lt=0,
                )
            ).aggregate(total=Sum('amount'), transaction_count=Count('id'))
        )

        deduction_cats = sorted(
            [
                {
                    'id': r['cls_id'],
                    'name': r['cls_name'],
                    'total': r['total'] or Decimal('0'),
                    'transaction_count': r['transaction_count'],
                }
                for r in payroll_expense_rows
            ],
            key=lambda c: abs(c['total']),
            reverse=True,
        )
        ded_unc_total = payroll_expense_unclassified['total'] or Decimal('0')
        ded_unc_count = payroll_expense_unclassified['transaction_count'] or 0
        deduction_section_total = sum(c['total'] for c in deduction_cats) + ded_unc_total
        deduction_section = {
            'type': 'payroll_expenses',
            'label': 'Payroll Deductions',
            'total': str(deduction_section_total),
            'transaction_count': sum(c['transaction_count'] for c in deduction_cats) + ded_unc_count,
            'categories': [
                {
                    'id': c['id'],
                    'name': c['name'],
                    'total': str(c['total']),
                    'transaction_count': c['transaction_count'],
                    'percent': _pct(c['total']),
                }
                for c in deduction_cats
            ],
            'uncategorized': {
                'total': str(ded_unc_total),
                'transaction_count': ded_unc_count,
                'percent': _pct(ded_unc_total),
            },
        }

        # ---------------------------------------------------------------
        # Section 3: Expenses from Other Accounts (non-payroll)
        # ---------------------------------------------------------------
        other_expense_rows = (
            _date_filter(
                Transaction.objects.filter(
                    location_classification__type=LocationClassification.TYPE_EXPENSE,
                ).exclude(account_id__in=payroll_account_ids)
            )
            .values(
                cls_id=models_F('location_classification__id'),
                cls_name=models_F('location_classification__name'),
            )
            .annotate(total=Sum('amount'), transaction_count=Count('id'))
        )
        other_expense_unclassified = (
            _date_filter(
                Transaction.objects.filter(
                    location_classification__isnull=True,
                    amount__lt=0,
                ).exclude(account_id__in=payroll_account_ids)
            ).aggregate(total=Sum('amount'), transaction_count=Count('id'))
        )

        other_cats = sorted(
            [
                {
                    'id': r['cls_id'],
                    'name': r['cls_name'],
                    'total': r['total'] or Decimal('0'),
                    'transaction_count': r['transaction_count'],
                }
                for r in other_expense_rows
            ],
            key=lambda c: abs(c['total']),
            reverse=True,
        )
        other_unc_total = other_expense_unclassified['total'] or Decimal('0')
        other_unc_count = other_expense_unclassified['transaction_count'] or 0
        other_section_total = sum(c['total'] for c in other_cats) + other_unc_total
        other_expense_section = {
            'type': 'other_expenses',
            'label': 'Expenses from Other Accounts',
            'total': str(other_section_total),
            'transaction_count': sum(c['transaction_count'] for c in other_cats) + other_unc_count,
            'categories': [
                {
                    'id': c['id'],
                    'name': c['name'],
                    'total': str(c['total']),
                    'transaction_count': c['transaction_count'],
                    'percent': _pct(c['total']),
                }
                for c in other_cats
            ],
            'uncategorized': {
                'total': str(other_unc_total),
                'transaction_count': other_unc_count,
                'percent': _pct(other_unc_total),
            },
        }

        # ---------------------------------------------------------------
        # Net change: payroll_total + all section outflows
        # ---------------------------------------------------------------
        net_change = (
            payroll_total
            + transfer_section_total
            + deduction_section_total
            + other_section_total
        )

        return Response({
            'date_from': date_from,
            'date_to': date_to,
            'payroll_total': str(payroll_total),
            'net_change': str(net_change),
            'sections': [transfer_section, deduction_section, other_expense_section],
        })
