"""Platform plumbing: pipeline authors do not need to import Dagster."""

import hashlib

import dagster as dg

from foundry_compat.runtime import LocalRuntime, local_spark


def dataset_key(path):
    return dg.AssetKey(path.strip("/").split("/"))


def build_definitions(pipeline, database):
    definitions = pipeline.ordered()

    @dg.resource
    def foundry_runtime(_context):
        spark = local_spark()
        try:
            yield LocalRuntime(database, spark)
        finally:
            spark.stop()

    def asset_for(definition):
        key = dataset_key(definition.output.path)

        @dg.multi_asset(
            name="transform_" + hashlib.sha256(definition.output.path.encode()).hexdigest()[:16],
            specs=[dg.AssetSpec(key, deps=[dataset_key(item.path) for item in definition.inputs.values()],
                               group_name="foundry_style", kinds={"python", "spark"},
                               metadata={"dataset": definition.output.path,
                                         "function": definition.function.__qualname__})],
            required_resource_keys={"foundry_runtime"},
        )
        def compute(context):
            report = context.resources.foundry_runtime.run(definition)
            context.log.info("Foundry-style transform: %s", report)
            return dg.MaterializeResult(asset_key=key, metadata=report)
        return compute

    outputs = {item.output.path for item in definitions}
    external = sorted({value.path for item in definitions for value in item.inputs.values()} - outputs)
    keys = [dataset_key(path) for path in sorted(outputs | set(external))]
    if len(set(keys)) != len(keys):
        raise ValueError("Dataset paths map to duplicate lineage keys")
    return dg.Definitions(
        assets=[asset_for(item) for item in definitions] + [
            dg.AssetSpec(dataset_key(path), metadata={"dataset": path}, group_name="source_inputs")
            for path in external
        ],
        resources={"foundry_runtime": foundry_runtime},
        jobs=[dg.define_asset_job("foundry_pipeline", selection=dg.AssetSelection.groups("foundry_style"),
                                  executor_def=dg.in_process_executor)],
    )
