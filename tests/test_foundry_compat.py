from dataclasses import replace
from datetime import datetime
from decimal import Decimal

import dagster as dg
import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, LongType, StringType, StructField, StructType, TimestampType

from foundry_compat.cli import STUDENT_SCHEMA
from foundry_compat.dagster_bridge import build_definitions, dataset_key
from foundry_compat.runtime import LocalRuntime, local_spark
from foundry_example.datasets.deduplicate import incremental_distinct
from foundry_example.pipeline import pipeline
from transforms.api import Input, Output, Pipeline, UnsupportedFeature, configure, incremental, transform, transform_df


@pytest.fixture(scope="module")
def spark():
    session = local_spark()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def runtime(tmp_path, spark):
    return LocalRuntime(tmp_path / "warehouse.sqlite", spark)


def ingest(runtime, identity, rows, mode="append"):
    return runtime.ingest("/demo/students", identity,
                          runtime.spark.createDataFrame(rows, STUDENT_SCHEMA), mode=mode)


def rows(runtime, dataset="/demo/unique_students"):
    return {tuple(row) for row in runtime.read(dataset).collect()}


def test_original_compute_body_incremental_dedup_and_snapshot_rebuild(runtime):
    ingest(runtime, "initial", [("A", "Brown"), ("A", "Brown"), ("B", None)])
    first = runtime.run(incremental_distinct)
    assert not first["is_incremental"]
    assert rows(runtime) == {("A", "Brown"), ("B", None)}
    ingest(runtime, "new", [("B", None), ("C", "Black")])
    second = runtime.run(incremental_distinct)
    assert second["is_incremental"] and second["written_rows"] == 1
    assert rows(runtime) == {("A", "Brown"), ("B", None), ("C", "Black")}
    before = runtime.inspect()
    assert runtime.run(incremental_distinct)["written_rows"] == 0
    assert runtime.inspect() == before
    # A source snapshot invalidates old output; previous must be empty on rebuild.
    ingest(runtime, "replace", [("D", "Blonde")], "replace")
    report = runtime.run(incremental_distinct)
    assert not report["is_incremental"]
    assert rows(runtime) == {("D", "Blonde")}


@pytest.mark.parametrize("point", ["after_output", "after_checkpoint", "after_commit"])
def test_output_and_checkpoint_atomicity_and_retries(runtime, point):
    ingest(runtime, "initial", [("A", "Brown")])
    runtime.run(incremental_distinct)
    ingest(runtime, "next", [("B", "Black")])
    before = runtime.inspect()

    def fail(here):
        if here == point:
            raise RuntimeError("simulated interruption")

    with pytest.raises(RuntimeError, match="simulated"):
        runtime.run(incremental_distinct, fault=fail)
    if point != "after_commit":
        assert runtime.inspect() == before
    result = runtime.run(incremental_distinct)
    assert result["written_rows"] == (0 if point == "after_commit" else 1)
    assert rows(runtime) == {("A", "Brown"), ("B", "Black")}
    assert runtime.read("/demo/unique_students").count() == 2


def test_require_incremental_and_semantic_version_rebuild(runtime):
    strict = replace(incremental_distinct, incremental_options=replace(
        incremental_distinct.incremental_options, require_incremental=True
    ))
    ingest(runtime, "initial", [("A", "Brown")])
    assert not runtime.run(strict)["is_incremental"]
    ingest(runtime, "replaced", [("B", "Black")], "replace")
    before = runtime.inspect()
    with pytest.raises(ValueError, match="require_incremental"):
        runtime.run(strict)
    assert runtime.inspect() == before
    bumped = replace(strict, incremental_options=replace(strict.incremental_options, semantic_version=2))
    assert not runtime.run(bumped)["is_incremental"]
    assert rows(runtime) == {("B", "Black")}


def test_discovery_generates_real_dagster_lineage_and_runs(runtime):
    assert len(pipeline.transforms) == 2
    definitions = build_definitions(pipeline, runtime.database)
    dg.Definitions.validate_loadable(definitions)
    graph = definitions.resolve_asset_graph()
    assert graph.get(dataset_key("/demo/unique_students")).parent_keys == {dataset_key("/demo/students")}
    assert graph.get(dataset_key("/demo/hair_counts")).parent_keys == {dataset_key("/demo/unique_students")}
    ingest(runtime, "initial", [("A", "Brown"), ("A", "Brown"), ("B", "Black")])
    with dg.DagsterInstance.ephemeral() as instance:
        result = definitions.resolve_job_def("foundry_pipeline").execute_in_process(
            instance=instance, resources={"foundry_runtime": runtime}
        )
    assert result.success and len(result.get_asset_materialization_events()) == 2
    assert rows(runtime, "/demo/hair_counts") == {("Brown", 1), ("Black", 1)}


def test_snapshot_reference_input_and_ctx(runtime):
    @incremental(snapshot_inputs=["reference"])
    @transform(events=Input("/events"), reference=Input("/reference"), result=Output("/enriched"))
    def enrich(ctx, events, reference, result):
        assert ctx.spark_session is runtime.spark
        result.write_dataframe(events.dataframe().join(reference.dataframe(), "code"))

    runtime.ingest("/events", "e1", runtime.spark.createDataFrame([("1", "x")], "id string, code string"))
    runtime.ingest("/reference", "r1", runtime.spark.createDataFrame([("x", "old")], "code string, label string"))
    assert not runtime.run(enrich)["is_incremental"]
    runtime.ingest("/reference", "r2", runtime.spark.createDataFrame([("x", "new")], "code string, label string"), mode="replace")
    runtime.ingest("/events", "e2", runtime.spark.createDataFrame([("2", "x")], "id string, code string"))
    assert runtime.run(enrich)["is_incremental"]
    assert rows(runtime, "/enriched") == {("x", "1", "old"), ("x", "2", "new")}


def test_source_receipts_and_schema_changes(runtime):
    assert ingest(runtime, "batch", [("A", "Brown")])
    assert not ingest(runtime, "batch", [("A", "Brown")])
    with pytest.raises(ValueError, match="identity"):
        ingest(runtime, "batch", [("B", "Black")])
    before = runtime.inspect()
    with pytest.raises(ValueError, match="schema"):
        runtime.ingest("/demo/students", "wrong-schema", runtime.spark.createDataFrame([(1,)], "id long"))
    assert runtime.inspect() == before


def test_incremental_transform_df_and_explicit_replace_output(runtime):
    @incremental()
    @transform_df(Output("/filtered"), source=Input("/demo/students"))
    def filter_students(source):
        return source.filter(F.col("hair") == "Brown")

    summary_schema = StructType([StructField("hair", StringType()), StructField("count", LongType())])

    @incremental()
    @transform(source=Input("/demo/students"), result=Output("/counts"))
    def count_students(source, result):
        additions = source.dataframe().groupBy("hair").count()
        previous = result.dataframe("previous", schema=summary_schema)
        totals = additions.unionByName(previous).groupBy("hair").agg(F.sum("count").alias("count"))
        result.set_mode("replace")
        result.write_dataframe(totals)

    ingest(runtime, "initial", [("A", "Brown"), ("B", "Black")])
    runtime.run(filter_students)
    runtime.run(count_students)
    ingest(runtime, "new", [("C", "Brown")])
    assert runtime.run(filter_students)["is_incremental"]
    assert runtime.run(count_students)["write_mode"] == "replace"
    assert rows(runtime, "/filtered") == {("A", "Brown"), ("C", "Brown")}
    assert rows(runtime, "/counts") == {("Brown", 2), ("Black", 1)}
    runtime.run(count_students)
    assert rows(runtime, "/counts") == {("Brown", 2), ("Black", 1)}


def test_abort_rolls_back_and_dagster_can_retry(runtime):
    ingest(runtime, "initial", [("A", "Brown")])
    fail = True

    @transform(source=Input("/demo/students"), result=Output("/retry"))
    def sometimes_abort(ctx, source, result):
        result.write_dataframe(source.dataframe())
        if fail:
            ctx.abort_job()

    local_pipeline = Pipeline()
    local_pipeline.add_transforms(sometimes_abort)
    job = build_definitions(local_pipeline, runtime.database).resolve_job_def("foundry_pipeline")
    before = runtime.inspect()
    with dg.DagsterInstance.ephemeral() as instance:
        assert not job.execute_in_process(instance=instance, resources={"foundry_runtime": runtime},
                                         raise_on_error=False).success
        assert runtime.inspect() == before
        fail = False
        assert job.execute_in_process(instance=instance, resources={"foundry_runtime": runtime}).success
    assert rows(runtime, "/retry") == {("A", "Brown")}


def test_storage_preserves_null_decimal_and_timestamp_precision(runtime):
    schema = StructType([StructField("id", StringType()), StructField("amount", DecimalType(18, 4)),
                         StructField("at", TimestampType())])
    values = [(None, Decimal("123.4567"), datetime(2026, 10, 13, 0, 0, 0, 123456))]
    runtime.ingest("/types", "t1", runtime.spark.createDataFrame(values, schema))
    assert [tuple(row) for row in runtime.read("/types").collect()] == values


def test_previous_schema_mismatch_and_unsupported_write_rollback(runtime):
    ingest(runtime, "initial", [("A", "Brown")])

    @incremental()
    @transform(source=Input("/demo/students"), result=Output("/check"))
    def validate(source, result):
        result.dataframe("previous", schema=STUDENT_SCHEMA)
        result.write_dataframe(source.dataframe())

    runtime.run(validate)
    # Passing a different schema on an incremental read must never silently cast it.
    bad_schema = StructType([StructField("wrong", StringType())])
    def wrong(source, result):
        result.dataframe("previous", schema=bad_schema)
    broken = replace(validate, function=wrong)
    # Retain callable identity to exercise previous-mode schema checking.
    wrong.__qualname__, wrong.__module__ = validate.function.__qualname__, validate.function.__module__
    before = runtime.inspect()
    with pytest.raises(ValueError, match="schema"):
        runtime.run(broken)
    assert runtime.inspect() == before

    @transform(source=Input("/demo/students"), result=Output("/unsupported"))
    def unsupported(source, result):
        result.write_dataframe(source.dataframe(), partition_cols=["hair"])
    with pytest.raises(UnsupportedFeature, match="write options"):
        runtime.run(unsupported)
    assert runtime.inspect() == before


def test_fail_closed_registration():
    with pytest.raises(UnsupportedFeature, match="options"):
        incremental(allow_retention=True)
    with pytest.raises(UnsupportedFeature, match="profiles"):
        configure(profile=["EXECUTOR_MEMORY_LARGE"])
    with pytest.raises(UnsupportedFeature, match="exactly one"):
        transform(source=Input("/in"), first=Output("/one"), second=Output("/two"))
    with pytest.raises(ValueError, match="names"):
        transform_df(Output("/out"), source=Input("/in"))(lambda other: other)

    one = transform_df(Output("/one"), source=Input("/two"))(lambda source: source)
    two = transform_df(Output("/two"), source=Input("/one"))(lambda source: source)
    cyclic = Pipeline()
    cyclic.add_transforms(one, two)
    with pytest.raises(ValueError, match="cycle"):
        cyclic.ordered()
    with pytest.raises(ValueError, match="Multiple transforms"):
        cyclic.add_transforms(replace(one, function=lambda source: source))
