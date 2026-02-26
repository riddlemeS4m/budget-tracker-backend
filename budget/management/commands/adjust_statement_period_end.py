from datetime import timedelta
from django.core.management.base import BaseCommand
from django.db import transaction as db_transaction
from budget.models import Account, Statement


class Command(BaseCommand):
    help = (
        "Shift Statement.period_end by a given number of days (positive to add, "
        "negative to subtract) for all statements, or a single account."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            '--days',
            type=int,
            required=True,
            metavar='DAYS',
            help='Number of days to add (positive) or subtract (negative) from period_end.',
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Print what would be updated without writing to the DB.',
        )
        parser.add_argument(
            '--account',
            type=int,
            dest='account_id',
            metavar='ACCOUNT_ID',
            help='Restrict the adjustment to a single account (by primary key).',
        )

    def handle(self, *args, **options):
        days = options['days']
        dry_run = options['dry_run']
        account_id = options.get('account_id')

        if days == 0:
            raise SystemExit(self.style.ERROR("DAYS must be non-zero."))

        direction = f"+{days}" if days > 0 else str(days)

        if account_id is not None:
            if not Account.objects.filter(pk=account_id).exists():
                raise SystemExit(self.style.ERROR(f"No account found with id={account_id}."))
            scope_label = f"account {account_id}"
        else:
            scope_label = "all accounts"

        self.stdout.write(
            f"Scanning statements ({scope_label}), shifting period_end by {direction} days…"
        )

        qs = Statement.objects.only('id', 'account_id', 'period_end').order_by('id')
        if account_id is not None:
            qs = qs.filter(account_id=account_id)

        delta = timedelta(days=days)
        bulk_updates: list[Statement] = []

        for stmt in qs.iterator(chunk_size=500):
            new_period_end = stmt.period_end + delta
            if dry_run:
                self.stdout.write(
                    f"  WOULD UPDATE  Statement {stmt.id} (account {stmt.account_id}): "
                    f"period_end {stmt.period_end} → {new_period_end}"
                )
            else:
                stmt.period_end = new_period_end
                bulk_updates.append(stmt)

        if dry_run:
            self.stdout.write(self.style.WARNING(
                f"\nDry run — no changes written. {len(bulk_updates) or qs.count()} "
                f"statement(s) would be updated."
            ))
            return

        chunk_size = 500
        updated = 0
        with db_transaction.atomic():
            for i in range(0, len(bulk_updates), chunk_size):
                chunk = bulk_updates[i:i + chunk_size]
                Statement.objects.bulk_update(chunk, ['period_end'])
                updated += len(chunk)
                self.stdout.write(f"  …{updated} statements updated so far")

        self.stdout.write(self.style.SUCCESS(
            f"\nDone. {updated} statement(s) updated (period_end shifted by {direction} days)."
        ))
