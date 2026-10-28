"""Run in a native Databricks notebook after uploading the pipeline sources.

Set PIPELINE_TRANSFORMATIONS_ROOT to /Workspace/Users/<owner>/CartaAzul/src/transformations.
Tests execute actual function bodies with only declarative decorators removed.
The actual common helper is loaded with only spark.conf.get calls redirected to
a test dictionary: notebook serverless forbids custom Spark configurations, while
the production pipeline explicitly supplies its configuration.
All test datasets are temporary views; no source files or persisted tables change.
"""
import ast
from decimal import Decimal
import importlib
import json
from pathlib import Path
import re
import sys
import types
import uuid

from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, StringType

if "PIPELINE_TRANSFORMATIONS_ROOT" not in globals():
    raise RuntimeError("Set PIPELINE_TRANSFORMATIONS_ROOT to the uploaded transformations directory first.")

_test_root = Path(PIPELINE_TRANSFORMATIONS_ROOT)
if str(_test_root) not in sys.path:
    sys.path.insert(0, str(_test_root))

from silver._schemas import CHECKS_SCHEMA, EMPLOYEES_SCHEMA, LOCATIONS_SCHEMA, REVENUE_CENTERS_SCHEMA

_test_prefix = "test_ca_" + uuid.uuid4().hex[:10] + "_"
_test_configuration = {
    "carta_azul.source_root": "/synthetic",
    "carta_azul.table_prefix": _test_prefix,
    "carta_azul.rule_version": "synthetic_test_v1",
    "carta_azul.reconciliation_tolerance": "0.05",
    "carta_azul.start_date": "2026-04-28",
    "carta_azul.end_date": "2026-04-28",
    "carta_azul.brand": "rmh",
}


def _test_conf_get(key, default=None):
    return _test_configuration.get(key, default)


class _RedirectNotebookConfiguration(ast.NodeTransformer):
    def visit_Call(self, node):
        self.generic_visit(node)
        function = node.func
        if (isinstance(function, ast.Attribute) and function.attr == "get"
            and isinstance(function.value, ast.Attribute) and function.value.attr == "conf"
            and isinstance(function.value.value, ast.Name) and function.value.value.id == "spark"):
            return ast.copy_location(ast.Call(
                func=ast.Name(id="_test_conf_get", ctx=ast.Load()),
                args=node.args, keywords=node.keywords), node)
        return node


_silver_package = importlib.import_module("silver")
_module_sentinel = object()
_prior_common_module = sys.modules.get("silver._common", _module_sentinel)
_prior_common_attribute = getattr(_silver_package, "_common", _module_sentinel)


def _restore_common_module():
    if _prior_common_module is _module_sentinel:
        sys.modules.pop("silver._common", None)
    else:
        sys.modules["silver._common"] = _prior_common_module
    if _prior_common_attribute is _module_sentinel:
        if hasattr(_silver_package, "_common"):
            delattr(_silver_package, "_common")
    else:
        setattr(_silver_package, "_common", _prior_common_attribute)


_common_source = _test_root / "silver/_common.py"
_common_tree = _RedirectNotebookConfiguration().visit(
    ast.parse(_common_source.read_text(encoding="utf-8"), filename=str(_common_source)))
ast.fix_missing_locations(_common_tree)
_ca_common = types.ModuleType("silver._common")
_ca_common.__file__ = str(_common_source)
_ca_common.__package__ = "silver"
_ca_common._test_conf_get = _test_conf_get
sys.modules["silver._common"] = _ca_common
setattr(_silver_package, "_common", _ca_common)
try:
    exec(compile(_common_tree, str(_common_source), "exec"), _ca_common.__dict__)
except BaseException:
    _restore_common_module()
    raise

_test_views = []
_test_results = []
_dec = lambda value: Decimal(str(value)) if value is not None else None


def _assert_test(condition, name):
    if not condition:
        raise AssertionError(name)
    _test_results.append(name)


def _source_frame(records, schema, filename):
    columns = StructType(schema.fields + [StructField("source_file", StringType(), False)])
    return spark.createDataFrame([dict(record, source_file=filename) for record in records], columns)


def _load_actual_functions(relative_path):
    source_path = _test_root / relative_path
    tree = ast.parse(source_path.read_text(encoding="utf-8"), filename=str(source_path))
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            node.decorator_list = []
    namespace = {"__name__": "carta_azul_synthetic_" + source_path.stem, "__file__": str(source_path)}
    exec(compile(tree, str(source_path), "exec"), namespace)
    return namespace


def _publish_temp(frame, table_suffix):
    name = _test_prefix + table_suffix
    frame.createOrReplaceTempView(name)
    _test_views.append(name)
    return frame


def _discount(amount, name="EMP 25%", actor=101, manager=0, discount_id=25):
    return dict(Amount=_dec(amount), DiscountId=discount_id, DiscountName=name,
                EmployeeId=actor, ManagerId=manager)


def _item(gross, sold, discounts=None, quantity=1, sequence=1):
    return dict(GrossPrice=_dec(gross), SoldPrice=_dec(sold), Quantity=_dec(quantity),
                Discounts=[] if discounts is None else discounts, ItemId=sequence,
                EmployeeId=101, Sequence=sequence)


def _payment(amount, tip=0, category="VISA", actor=101, card=True):
    return dict(Amount=_dec(amount), Gratuity=_dec(tip), PayCatName=category,
                EmployeeId=actor, PaymentCategoryId=1, PayCatNo=1,
                IsCreditCard=card, IsCashDebit=False, IsGiftCard=False,
                IsGiftCert=False, IsNonPOS=False, IsPettyCash=False)


def _check(check_id, items=None, payments=None, discounts=None, tax=0):
    return dict(Id=check_id, LocationId=10, RevenueCenterId=1,
                BusinessDate="2026-04-28", OpenTime="2026-04-28T12:00:00",
                CloseTime="2026-04-28T13:00:00", ServiceCharge=_dec(0),
                TrafficCount=_dec(1), ItemsSold=[] if items is None else items,
                Payments=[] if payments is None else payments,
                Discounts=[] if discounts is None else discounts,
                Tax=[dict(Amount=_dec(tax), TaxCategoryId=1, TaxCategoryName="Sales")],
                Voids=[])


_fixture_path = "/synthetic/facts/checks/brand=rmh/dt=2026-04-28/checks.json"
_fixture_rows = [
    # Gross 100, sold 75 already includes 25 item discounts, ticket discount 5.
    # Net 70 + tax 7 = payment 87 + raw gratuity -10. Quantity must not multiply price.
    _check(1,
           [_item(60, 45, [_discount(15, manager=0)], quantity=2),
            _item(40, 30, [_discount(10, manager=101)], sequence=2)],
           [_payment(50, -6), _payment(37, -4)],
           [_discount(5, name="EMP 10%", manager=102, discount_id=10)], tax=7),
    # Negative GC Sold payment is retained; actor 103 is an ambiguous reference.
    _check(2, [_item(10, 10)],
           [_payment(-5, category="GC Sold", actor=103, card=False), _payment(15)]),
    _check(3),
    dict(_check(4), ItemsSold=None, Payments=None, Discounts=None, Tax=None),
    # Same business key, conflicting payloads: one deterministic inspection row,
    # source_duplicate_count 2, both excluded from analytical eligibility.
    _check(5, [_item(20, 20)], [_payment(20)]),
    _check(5, [_item(25, 25)], [_payment(25)]),
    _check(6, [_item(None, 20)], [_payment(20)]),
    # Opposite tip sign can reconcile, but must remain outside ordinary comparisons.
    _check(7, [_item(20, 20)], [_payment(15, tip=5)]),
    # Missing and blank labels cannot be classified as non-employee peers merely
    # because the monetary fields reconcile. Discount amounts still sum to five.
    _check(8, [_item(20, 20)], [_payment(15)], [
        _discount(2, name=None, manager=102, discount_id=28),
        _discount(3, name="   ", manager=102, discount_id=38),
    ]),
    # Unknown card flag is not proof of no card; blank category also needs review.
    _check(9, [_item(20, 20)], [_payment(20, category="   ", card=None)]),
]


def _run_gold_sql(relative_path, config):
    source = (_test_root / relative_path).read_text(encoding="utf-8")
    # Keep the actual SELECT/CTEs. Constraints are exercised by the real pipeline;
    # this harness validates query semantics without creating materialized views.
    query = source.split("\nAS\n", 1)[1].rstrip().rstrip(";")
    query = re.sub(r"\$\{([^}]+)\}", lambda match: str(config[match.group(1)]), query)
    frame = spark.sql(query)
    suffix = Path(relative_path).stem.replace("mv_", "gold_", 1)
    return _publish_temp(frame, suffix)


try:
    _parsed_fractional_void = spark.range(1).select(F.from_json(
        F.lit('{"Voids":[{"Quantity":0.5,"Amount":1.25}]}'),
        CHECKS_SCHEMA, {"mode": "FAILFAST"},
    ).alias("parsed")).first()["parsed"]["Voids"][0]
    _assert_test(_parsed_fractional_void["Quantity"] == Decimal("0.5000")
                 and _parsed_fractional_void["Amount"] == Decimal("1.2500"),
                 "Actual strict source schema preserves fractional void quantities and amounts as exact Decimal values")
    _raw = _source_frame(_fixture_rows, CHECKS_SCHEMA, _fixture_path)
    _checks_functions = _load_actual_functions("silver/mv_checks.py")
    _checks_functions["read_json"] = lambda paths, schema: _raw
    _checks = _publish_temp(_checks_functions["silver_checks"](), "silver_checks")
    _observed = {row.check_id: row.asDict() for row in _checks.collect()}
    _assert_test(len(_observed) == 9, "Canonical tickets preserve nine distinct keys from ten source rows")
    _one = _observed[1]
    _assert_test(_one["gross_sales_amount"] == _dec(100) and _one["sold_sales_amount"] == _dec(75),
                 "Line money is not multiplied by Quantity")
    _assert_test(_one["item_discount_amount"] == _dec(25) and _one["net_sales_amount"] == _dec(70),
                 "Net sales subtract ticket discount once and never double-subtract item discounts")
    _assert_test(_one["tip_amount"] == _dec(10) and _one["reconciliation_difference"] == _dec(0)
                 and _one["reconciliation_pass"] and _one["item_discount_reconciliation_pass"],
                 "Negative raw gratuity normalizes to positive tip and empirical settlement equation reconciles")
    _assert_test(_observed[3]["monetary_fields_complete"] and not _observed[4]["monetary_fields_complete"],
                 "Empty lists remain distinguishable from unknown null lists")
    _assert_test(not _observed[3]["analytic_eligible"] and not _observed[4]["analytic_eligible"],
                 "Empty and null item/payment populations are analytically ineligible")
    _assert_test(_observed[5]["source_duplicate_count"] == 2 and not _observed[5]["analytic_eligible"],
                 "Conflicting source keys are counted and excluded rather than silently accepted")
    _assert_test(not _observed[6]["monetary_fields_complete"] and not _observed[6]["analytic_eligible"],
                 "Missing monetary values cannot become analytically observed zero")

    # Reversing source row order must not change the canonical chosen payload.
    _checks_functions["read_json"] = lambda paths, schema: _source_frame(
        list(reversed(_fixture_rows)), CHECKS_SCHEMA, _fixture_path)
    _again = _checks_functions["silver_checks"]()
    _left = _checks.select("ticket_key", "source_record_hash").orderBy("ticket_key").collect()
    _right = _again.select("ticket_key", "source_record_hash").orderBy("ticket_key").collect()
    _assert_test(_left == _right, "Canonical record choice is stable when raw input row order changes")

    _reference_functions = _load_actual_functions("silver/mv_reference_dimensions.py")
    _references = {
        "locations": _source_frame([dict(Id=10, Name="Synthetic location", TimeZoneName="America/Chicago")],
                                   LOCATIONS_SCHEMA, "/synthetic/dimensions/locations/locations.json"),
        "revenue_centers": _source_frame([dict(Id=1, Name="Synthetic dining")],
                                         REVENUE_CENTERS_SCHEMA, "/synthetic/dimensions/revenue_centers/revenue_centers.json"),
        "employees": _source_frame([
            dict(Id=101, LocationId=10, HomeLocationId=10, Deleted=True, Borrowed=False),
            dict(Id=102, LocationId=10, HomeLocationId=10, Deleted=False, Borrowed=False),
            dict(Id=103, LocationId=10, HomeLocationId=10, Deleted=False, Borrowed=False),
            dict(Id=103, LocationId=11, HomeLocationId=11, Deleted=True, Borrowed=False),
        ], EMPLOYEES_SCHEMA, "/synthetic/dimensions/employees/employees.json"),
    }

    def _read_synthetic_reference(paths, schema):
        return next(frame for name, frame in _references.items() if f"/dimensions/{name}/" in str(paths))

    _reference_functions["read_json"] = _read_synthetic_reference
    for _suffix in ("silver_locations", "silver_revenue_centers", "silver_employees"):
        _publish_temp(_reference_functions[_suffix](), _suffix)

    _discount_functions = _load_actual_functions("silver/mv_discounts.py")
    _discounts = _publish_temp(_discount_functions["silver_discounts"](), "silver_discounts")
    _payment_functions = _load_actual_functions("silver/mv_payments.py")
    _payments = _publish_temp(_payment_functions["silver_payments"](), "silver_payments")
    _discount_rows = _discounts.collect()
    _payment_rows = _payments.collect()
    _assert_test(len([row for row in _discount_rows if row.check_id == 1]) == 3
                 and len([row for row in _payment_rows if row.check_id == 1]) == 2,
                 "Two items and two payments yield three discount events and two payments without Cartesian multiplication")
    _assert_test(not any(row.check_id in (3, 4) for row in _discount_rows + _payment_rows),
                 "Empty/null lists produce no fabricated event rows")
    _assert_test(len({row.discount_event_key for row in _discount_rows}) == len(_discount_rows)
                 and len({row.payment_event_key for row in _payment_rows}) == len(_payment_rows),
                 "Scope and array-position event keys are unique")
    _zero_approver = next(row for row in _discount_rows if row.approver_employee_id == 0)
    _same_actor = next(row for row in _discount_rows if row.approver_employee_id == 101)
    _assert_test(not _zero_approver.approver_reference_resolved
                 and _same_actor.actor_reference_resolved and _same_actor.approver_reference_resolved,
                 "Manager zero remains unresolved while same-actor historical references resolve")
    _assert_test(all(row.actor_reference_resolved for row in _discount_rows),
                 "Historically Deleted employee references remain available")
    _negative = next(row for row in _payment_rows if row.amount < 0)
    _assert_test(_negative.amount == _dec(-5) and _negative.payment_category_name == "GC Sold"
                 and _negative.has_negative_amount and not _negative.actor_reference_resolved,
                 "Negative GC Sold event is preserved and ambiguous employee Id does not resolve")

    _quality_functions = _load_actual_functions("silver/mv_quality_issues.py")
    _issues = _publish_temp(_quality_functions["silver_quality_issues"](), "silver_quality_issues")
    _issue_rows = _issues.select("check_id" if "check_id" in _issues.columns else "ticket_key", "issue_code", "severity").collect()
    _assert_test(any(row.issue_code == "APPROVER_ZERO" and row.severity == "INFO" for row in _issue_rows),
                 "Manager zero generates contextual INFO rather than a missing-authorization allegation")
    _assert_test(any(row.issue_code == "DUPLICATE_TICKET_KEY" and row.severity == "ERROR" for row in _issue_rows),
                 "Source duplicate appears in the explained quality output")
    _assert_test(any(row.ticket_key == _observed[8]["ticket_key"]
                     and row.issue_code == "INCOMPLETE_DISCOUNT" and row.severity == "ERROR"
                     for row in _issue_rows),
                 "Null or blank discount labels produce explained incomplete-discount quality issues")
    _assert_test(any(row.ticket_key == _observed[9]["ticket_key"]
                     and row.issue_code == "MISSING_PAYMENT_CATEGORY" and row.severity == "WARNING"
                     for row in _issue_rows)
                 and any(row.ticket_key == _observed[9]["ticket_key"]
                         and row.issue_code == "MISSING_PAYMENT_CARD_FLAG" and row.severity == "WARNING"
                         for row in _issue_rows),
                 "Blank payment category and unknown card flag both generate contextual warnings")

    _config = {
        "carta_azul.table_prefix": _test_prefix,
        "carta_azul.rule_version": "synthetic_test_v1",
        "carta_azul.expected_start_date": "2026-04-28",
        "carta_azul.expected_end_date": "2026-04-28",
        "carta_azul.expected_source_files": "1",
        "carta_azul.reconciliation_tolerance": "0.05",
        "carta_azul.review_percentile": "0.99",
        "carta_azul.review_min_cohort": "50",
    }
    _features = _run_gold_sql("gold/mv_check_features.sql", _config)
    _feature_rows = {row.check_id: row.asDict() for row in _features.collect()}
    _assert_test(len(_feature_rows) == 9 and _feature_rows[1]["ordinary_comparison_eligible"],
                 "Actual Gold SQL preserves one feature row per canonical ticket and a valid ordinary comparison")
    _assert_test(not _feature_rows[2]["ordinary_comparison_eligible"]
                 and not _feature_rows[5]["monetary_analysis_eligible"]
                 and not _feature_rows[7]["ordinary_comparison_eligible"],
                 "Gold excludes special negative payments, duplicated sources and unexpected positive gratuity from intended populations")
    _eight = _feature_rows[8]
    _assert_test(_eight["monetary_analysis_eligible"] and not _eight["ordinary_comparison_eligible"]
                 and not _eight["discount_classification_complete"]
                 and _eight["incomplete_discount_event_count"] == 2
                 and _eight["employee_discount_amount"] is None,
                 "Reconciled unknown-label discounts remain monetary data but cannot become non-employee peers or employee amount zero")
    _nine = _feature_rows[9]
    _assert_test(_nine["monetary_analysis_eligible"] and not _nine["ordinary_comparison_eligible"]
                 and _nine["payment_credit_card_flag_missing_count"] == 1
                 and _nine["payment_category_missing_count"] == 1,
                 "Reconciled missing card/category context remains monetary data but cannot enter ordinary comparison cohorts")
    _daily = _run_gold_sql("gold/mv_discount_daily.sql", _config)
    _assert_test(_daily.count() > 0, "Actual daily summary SQL executes with explicit eligible population denominators")
    _cases = _run_gold_sql("gold/mv_review_cases.sql", _config)
    _assert_test(_cases.where("case_category = 'EXPLORATORY_COMPARISON'").count() == 0,
                 "Insufficient cohort support produces no statistical review cases")
    _snapshot = _run_gold_sql("gold/mv_pipeline_quality.sql", _config).first().asDict()
    _assert_test(_snapshot["canonical_ticket_count"] == 9 and _snapshot["source_record_count"] == 10
                 and _snapshot["duplicate_ticket_key_count"] == 1,
                 "Actual quality SQL separates canonical ticket counts from original duplicate source rows")
    _assert_test(_snapshot["payment_event_count"] == _snapshot["expected_payment_event_count"]
                 and _snapshot["discount_event_count"] == _snapshot["expected_discount_event_count"]
                 and _snapshot["snapshot_quality_status"] == "COMPLETE_WITH_ISSUES_TO_REVIEW",
                 "Actual quality SQL enforces array cardinality without hiding known synthetic issues")

    carta_azul_synthetic_test_results = {"status": "PASS", "count": len(_test_results), "tests": _test_results}
    print(json.dumps(carta_azul_synthetic_test_results, ensure_ascii=False))
finally:
    try:
        for _view in reversed(_test_views):
            spark.catalog.dropTempView(_view)
    finally:
        _restore_common_module()
