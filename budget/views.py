import csv
import io
from collections import defaultdict
from decimal import Decimal
from django.db.models import Count, Sum, F as models_F
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

    location_subclassification_id = query_params.get("location_subclassification")
    if location_subclassification_id:
        queryset = queryset.filter(location_subclassification_id=int(location_subclassification_id))

    time_classification_id = query_params.get("time_classification")
    if time_classification_id:
        queryset = queryset.filter(time_classification_id=int(time_classification_id))

    person_classification_id = query_params.get("person_classification")
    if person_classification_id:
        queryset = queryset.filter(person_classification_id=int(person_classification_id))

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


class StatementViewSet(viewsets.ModelViewSet):
    queryset = Statement.objects.order_by('id')
    serializer_class = StatementSerializer


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
    # Organise rows: type -> category -> subcategory -> total
    tree = {}  # type -> {cat_key -> {sub_key -> total}}
    cat_meta = {}   # cat_key -> {id, name, type}
    sub_meta = {}   # sub_key -> {id, name}

    UNCLASSIFIED_CAT = (None, "Unclassified")
    UNCATEGORIZED_SUB = (None, "Uncategorized")

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

        rows = qs.values(
            cls_id=models_F('location_classification__id'),
            cls_name=models_F('location_classification__name'),
            cls_type=models_F('location_classification__type'),
            sub_id=models_F('location_subclassification__id'),
            sub_name=models_F('location_subclassification__name'),
        ).annotate(total=Sum('amount'))

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
