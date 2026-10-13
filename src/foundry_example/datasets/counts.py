"""A second transform, written entirely with the familiar DataFrame API."""

from transforms.api import Input, Output, transform_df


@transform_df(
    Output("/demo/hair_counts"),
    students=Input("/demo/unique_students"),
)
def hair_counts(students):
    return students.groupBy("hair").count()
