from datetime import timedelta
from django.core.management.base import BaseCommand
from django.db import transaction as db_transaction
from budget.models import Account, Statement


class Command(BaseCommand):
    help = (
        "Backfill Statement.period_start where it is null, setting it to the "
        "period_end of the immediately preceding statement for the same account, "
        "provided that predecessor closed within 32 days of the current statement."
    )

    def add_arguments(self, parser):
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
            help='Restrict the backfill to a single account (by primary key).',
        )

    def handle(self, *args, **options):
        dry_run = options['dry_run']
        account_id = options.get('account_id')

        if account_id is not None:
            if not Account.objects.filter(pk=account_id).exists():
                raise SystemExit(self.style.ERROR(f"No account found with id={account_id}."))
            scope_label = f"account {account_id}"
        else:
            scope_label = "all accounts"

        self.stdout.write(f"Scanning statements with no period_start ({scope_label})…")

        # Fetch all statements for the relevant accounts, ordered oldest-first
        # so we can resolve predecessors in one pass per account.
        qs = (
            Statement.objects
            .select_related('account')
            .order_by('account_id', 'period_end')
        )
        if account_id is not None:
            qs = qs.filter(account_id=account_id)

        # Group into a dict: account_id → [statements in ascending period_end order]
        from collections import defaultdict
        by_account: dict[int, list[Statement]] = defaultdict(list)
        for stmt in qs.iterator(chunk_size=500):
            by_account[stmt.account_id].append(stmt)

        bulk_updates: list[Statement] = []
        skipped_no_predecessor = 0
        skipped_too_far = 0

        for acct_id, stmts in by_account.items():
            for i, stmt in enumerate(stmts):
                if stmt.period_start is not None:
                    continue  # already has an open date

                if i == 0:
                    # No predecessor exists for the earliest statement
                    skipped_no_predecessor += 1
                    continue

                predecessor = stmts[i - 1]
                delta = (stmt.period_end - predecessor.period_end).days

                if delta > 32:
                    skipped_too_far += 1
                    if dry_run:
                        self.stdout.write(
                            f"  SKIP  Statement {stmt.id} (account {acct_id}): "
                            f"predecessor period_end {predecessor.period_end} is "
                            f"{delta} days before {stmt.period_end} (> 32)"
                        )
                    continue

                if dry_run:
                    self.stdout.write(
                        f"  WOULD SET  Statement {stmt.id} (account {acct_id}): "
                        f"period_start = {predecessor.period_end} "
                        f"(predecessor period_end, {delta}d gap)"
                    )
                else:
                    stmt.period_start = predecessor.period_end
                    bulk_updates.append(stmt)

        if dry_run:
            self.stdout.write(self.style.WARNING(
                f"\nDry run — no changes written.\n"
                f"  Would update:          {len(bulk_updates) or '(run without --dry-run to count)'}\n"
                f"  Skipped (no predecessor): {skipped_no_predecessor}\n"
                f"  Skipped (gap > 32 days):  {skipped_too_far}"
            ))
            return

        chunk_size = 500
        updated = 0
        with db_transaction.atomic():
            for i in range(0, len(bulk_updates), chunk_size):
                chunk = bulk_updates[i:i + chunk_size]
                Statement.objects.bulk_update(chunk, ['period_start'])
                updated += len(chunk)
                self.stdout.write(f"  …{updated} statements updated so far")

        self.stdout.write(self.style.SUCCESS(
            f"\nDone. {updated} statement(s) backfilled. "
            f"Skipped {skipped_no_predecessor} (no predecessor), "
            f"{skipped_too_far} (gap > 32 days)."
        ))
