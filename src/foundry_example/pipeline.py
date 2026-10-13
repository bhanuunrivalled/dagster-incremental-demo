from transforms.api import Pipeline
from foundry_example import datasets

pipeline = Pipeline()
pipeline.discover_transforms(datasets)
