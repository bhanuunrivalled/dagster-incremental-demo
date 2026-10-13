"""Familiar authoring API; unsupported features raise instead of becoming no-ops."""

import importlib
import inspect
import pkgutil
from dataclasses import dataclass, replace
from types import ModuleType
from typing import Callable


class UnsupportedFeature(NotImplementedError):
    pass


@dataclass(frozen=True)
class Input:
    path: str

    def __post_init__(self):
        if not isinstance(self.path, str) or not self.path.strip():
            raise ValueError("Input requires a nonempty dataset path or identifier")


@dataclass(frozen=True)
class Output:
    path: str

    def __post_init__(self):
        if not isinstance(self.path, str) or not self.path.strip():
            raise ValueError("Output requires a nonempty dataset path or identifier")


@dataclass(frozen=True)
class IncrementalOptions:
    semantic_version: int = 1
    require_incremental: bool = False
    snapshot_inputs: tuple[str, ...] = ()


@dataclass(frozen=True)
class Transform:
    function: Callable
    inputs: dict[str, Input]
    output_name: str
    output: Output
    dataframe_function: bool = False
    incremental_options: IncrementalOptions | None = None


def _definition(function, inputs, output_name, output, dataframe_function=False):
    if not inputs or any(not isinstance(value, Input) for value in inputs.values()):
        raise UnsupportedFeature("This prototype requires one or more Input datasets")
    if output.path in {value.path for value in inputs.values()}:
        raise ValueError("An output cannot also be an input of the same transform")
    parameters = inspect.signature(function).parameters
    bindings = set(inputs) | (set() if dataframe_function else {output_name})
    if "ctx" in bindings or not bindings <= parameters.keys():
        raise ValueError("Dataset argument names must match the compute function; ctx is reserved")
    for name, parameter in parameters.items():
        if parameter.kind not in (parameter.POSITIONAL_OR_KEYWORD, parameter.KEYWORD_ONLY):
            raise UnsupportedFeature("Variadic and positional-only compute parameters are unsupported")
        if name not in bindings | {"ctx"} and parameter.default is parameter.empty:
            raise ValueError(f"Unbound required compute parameter: {name}")
    return Transform(function, dict(inputs), output_name, output, dataframe_function)


def transform(**bindings):
    outputs = {name: value for name, value in bindings.items() if isinstance(value, Output)}
    inputs = {name: value for name, value in bindings.items() if isinstance(value, Input)}
    if len(outputs) != 1 or len(inputs) + len(outputs) != len(bindings):
        raise UnsupportedFeature("@transform currently supports Input bindings and exactly one Output")
    output_name, output = next(iter(outputs.items()))
    return lambda function: _definition(function, inputs, output_name, output)


def transform_df(output, **inputs):
    if not isinstance(output, Output):
        raise TypeError("@transform_df requires an Output as its first argument")
    return lambda function: _definition(function, inputs, "_result", output, True)


def incremental(*, semantic_version=1, require_incremental=False, snapshot_inputs=None, **unsupported):
    if unsupported:
        raise UnsupportedFeature(f"Unsupported @incremental options: {', '.join(sorted(unsupported))}")
    if type(semantic_version) is not int or semantic_version < 1:
        raise ValueError("semantic_version must be a positive integer")
    if type(require_incremental) is not bool:
        raise TypeError("require_incremental must be a bool")
    if snapshot_inputs is not None and (not isinstance(snapshot_inputs, (list, tuple)) or
                                       any(not isinstance(name, str) for name in snapshot_inputs)):
        raise TypeError("snapshot_inputs must be a sequence of input argument names")
    snapshots = tuple(snapshot_inputs or ())

    def decorate(definition):
        if not isinstance(definition, Transform):
            raise TypeError("Place @incremental above @transform or @transform_df")
        if definition.incremental_options is not None:
            raise ValueError("Duplicate @incremental decorator")
        if not set(snapshots) <= definition.inputs.keys():
            raise ValueError("snapshot_inputs contains unknown input arguments")
        if set(snapshots) == definition.inputs.keys():
            raise UnsupportedFeature("At least one non-snapshot input is required")
        return replace(definition, incremental_options=IncrementalOptions(
            semantic_version, require_incremental, snapshots
        ))
    return decorate


class Pipeline:
    def __init__(self):
        self.transforms: list[Transform] = []

    def add_transforms(self, *definitions):
        pending = list(self.transforms)
        for definition in definitions:
            if not isinstance(definition, Transform):
                raise TypeError("Pipeline accepts decorated transforms")
            if any(item is definition for item in pending):
                continue
            if any(item.output.path == definition.output.path for item in pending):
                raise ValueError(f"Multiple transforms produce {definition.output.path}")
            pending.append(definition)
        self.transforms = pending

    def discover_transforms(self, module: ModuleType):
        modules = [module]
        if hasattr(module, "__path__"):
            modules.extend(importlib.import_module(item.name) for item in sorted(
                pkgutil.walk_packages(module.__path__, module.__name__ + "."), key=lambda item: item.name
            ))
        self.add_transforms(*(value for item in modules for value in vars(item).values()
                              if isinstance(value, Transform)))

    def ordered(self):
        remaining = list(self.transforms)
        ordered = []
        while remaining:
            produced = {item.output.path for item in remaining}
            ready = [item for item in remaining if not produced.intersection(
                value.path for value in item.inputs.values()
            )]
            if not ready:
                raise ValueError("Pipeline contains a dataset dependency cycle")
            ordered.extend(ready)
            remaining = [item for item in remaining if item not in ready]
        return ordered


def configure(*args, **kwargs):
    raise UnsupportedFeature("Compute profiles need an explicit backend mapping; @configure is not implemented")
