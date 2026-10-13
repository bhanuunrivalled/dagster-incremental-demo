"""Public-doc deduplication pattern; synthetic paths, unchanged compute body.

Reference: https://www.palantir.com/docs/foundry/transforms-python-spark/incremental-examples#merge-and-append
"""

from transforms.api import Input, Output, incremental, transform


@incremental()
@transform(
    students=Input("/demo/students"),
    processed=Output("/demo/unique_students"),
)
def incremental_distinct(students, processed):
    new_students_df = students.dataframe()
    processed.write_dataframe(
        new_students_df.distinct().subtract(
            processed.dataframe('previous', schema=new_students_df.schema)
        )
    )
