from collections import defaultdict
from django.core.management.base import BaseCommand
from django.db import transaction as db_transaction
from budget.models import Account, LocationClassification, LocationSubClassification, Transaction


# Account types where a positive amount = income, negative = expense
STANDARD_SIGN_TYPES = {
    Account.TYPE_CHECKING,
    Account.TYPE_SAVINGS,
    Account.TYPE_CREDIT_CARD,
    Account.TYPE_INVESTMENT,
    Account.TYPE_LOAN,
}

TRANSFER_CATEGORY = 'N/A'


def infer_type_from_account_and_amount(account_type: str, amount) -> str:
    if account_type in STANDARD_SIGN_TYPES:
        if amount is not None and amount > 0:
            return LocationClassification.TYPE_INCOME
        return LocationClassification.TYPE_EXPENSE
    # TYPE_OTHER or anything unexpected — default to expense
    return LocationClassification.TYPE_EXPENSE


class Command(BaseCommand):
    help = (
        "One-time migration: derives LocationClassification type from account "
        "type + amount sign, creates LC/LSC records, then back-fills FK fields "
        "on all transactions."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Print what would be created/updated without writing to the DB.',
        )

    def handle(self, *args, **options):
        dry_run = options['dry_run']

        # ------------------------------------------------------------------
        # Step 1: Scan transactions and collect votes for each category's type.
        # Structure: { category: { type: count } }
        # Also collect subcategories per category.
        # ------------------------------------------------------------------
        self.stdout.write("Scanning transactions…")

        # { category: { 'income': n, 'expense': n, 'transfer': n } }
        type_votes: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        # { category: set(subcategories) }
        subcats_by_cat: dict[str, set] = defaultdict(set)

        qs = (
            Transaction.objects
            .select_related('account')
            .only('category', 'subcategory', 'amount', 'account__type')
        )

        for tx in qs.iterator(chunk_size=500):
            cat = (tx.category or '').strip()
            if not cat:
                continue

            subcat = (tx.subcategory or '').strip() or None
            subcats_by_cat[cat].add(subcat)

            if cat == TRANSFER_CATEGORY:
                type_votes[cat][LocationClassification.TYPE_TRANSFER] += 1
            else:
                t = infer_type_from_account_and_amount(tx.account.type, tx.amount)
                type_votes[cat][t] += 1

        self.stdout.write(f"  Found {len(type_votes)} unique categories.")

        # ------------------------------------------------------------------
        # Step 2: Resolve each category to a single type, flagging conflicts.
        # ------------------------------------------------------------------
        resolved: dict[str, str] = {}   # category → winning type
        conflicts: list[str] = []

        for cat, votes in type_votes.items():
            if len(votes) == 1:
                resolved[cat] = next(iter(votes))
            else:
                # Conflict: multiple types inferred for this category.
                # Pick the plurality winner but flag it for review.
                winning_type = max(votes, key=lambda t: votes[t])
                resolved[cat] = winning_type
                conflicts.append(cat)

        if conflicts:
            self.stdout.write(self.style.WARNING(
                f"\n  ⚠️  {len(conflicts)} category/categories had conflicting type votes "
                f"(plurality used, recommend manual audit):"
            ))
            for cat in sorted(conflicts):
                votes = type_votes[cat]
                vote_str = ', '.join(f"{t}={n}" for t, n in votes.items())
                self.stdout.write(f"    {cat!r} → {resolved[cat]} ({vote_str})")
            self.stdout.write("")

        # ------------------------------------------------------------------
        # Dry run: just print the plan and exit.
        # ------------------------------------------------------------------
        if dry_run:
            self.stdout.write("Proposed LocationClassification records:")
            for cat in sorted(resolved):
                t = resolved[cat]
                subcats = subcats_by_cat[cat]
                subcat_list = sorted(s for s in subcats if s)
                self.stdout.write(f"  [{t}] {cat!r}")
                for s in subcat_list:
                    self.stdout.write(f"      > {s!r}")
            self.stdout.write(self.style.WARNING("\nDry run — no changes written."))
            return

        # ------------------------------------------------------------------
        # Step 3: get-or-create LocationClassification rows.
        # If one already exists (by name), use it as-is without updating type.
        # ------------------------------------------------------------------
        self.stdout.write("Creating LocationClassification records…")

        cat_to_lc: dict[str, LocationClassification] = {}

        with db_transaction.atomic():
            for cat, inferred_type in resolved.items():
                lc, created = LocationClassification.objects.get_or_create(
                    name=cat,
                    defaults={'type': inferred_type},
                )
                if created:
                    self.stdout.write(f"  CREATED [{lc.type}] {lc.name!r}")
                else:
                    self.stdout.write(
                        f"  EXISTS  [{lc.type}] {lc.name!r}"
                        + (f" (inferred {inferred_type!r}, kept existing)" if lc.type != inferred_type else "")
                    )
                cat_to_lc[cat] = lc

            # ---------------------------------------------------------------
            # Step 4: get-or-create LocationSubClassification rows.
            # ---------------------------------------------------------------
            self.stdout.write("Creating LocationSubClassification records…")

            pair_to_lsc: dict[tuple[str, str | None], LocationSubClassification | None] = {}

            for cat, subcats in subcats_by_cat.items():
                lc = cat_to_lc.get(cat)
                if lc is None:
                    continue
                for subcat in subcats:
                    if subcat is None:
                        pair_to_lsc[(cat, None)] = None
                        continue
                    lsc, created = LocationSubClassification.objects.get_or_create(
                        location_classification=lc,
                        name=subcat,
                    )
                    if created:
                        self.stdout.write(f"  CREATED LSC: {lc.name!r} > {lsc.name!r}")
                    pair_to_lsc[(cat, subcat)] = lsc

            # ---------------------------------------------------------------
            # Step 5: back-fill FK fields on transactions still missing them.
            # ---------------------------------------------------------------
            self.stdout.write("Back-filling transaction FK fields…")

            bulk_updates = []
            updated = 0
            chunk_size = 500

            qs = (
                Transaction.objects
                .filter(location_classification__isnull=True)
                .only('id', 'category', 'subcategory')
            )

            for tx in qs.iterator(chunk_size=chunk_size):
                cat = (tx.category or '').strip()
                if not cat:
                    continue
                subcat = (tx.subcategory or '').strip() or None

                tx.location_classification = cat_to_lc.get(cat)
                tx.location_subclassification = pair_to_lsc.get((cat, subcat))
                bulk_updates.append(tx)

                if len(bulk_updates) >= chunk_size:
                    Transaction.objects.bulk_update(
                        bulk_updates,
                        ['location_classification', 'location_subclassification'],
                    )
                    updated += len(bulk_updates)
                    bulk_updates = []
                    self.stdout.write(f"  …{updated} transactions updated so far")

            if bulk_updates:
                Transaction.objects.bulk_update(
                    bulk_updates,
                    ['location_classification', 'location_subclassification'],
                )
                updated += len(bulk_updates)

        self.stdout.write(self.style.SUCCESS(
            f"\nDone. {updated} transaction(s) back-filled."
        ))