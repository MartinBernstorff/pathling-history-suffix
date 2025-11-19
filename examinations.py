from pathling import DataSource, PathlingContext


def main():
    pc = PathlingContext.create()
    df1 = pc.read.delta("delta-lake")
    df2 = pc.read.delta("delta-lake-2")

    for df in [df1, df2]:
        print_examination(df)

    pass


def print_examination(df: DataSource):
    view = df.view(
        resource="Communication",
        select=[{"column": [{"path": "getResourceKey()", "name": "resource_id"}]}],
    )

    print(view.count())

    resource = view.filter("resource_id LIKE '%Communication/1000311767%'").collect()
    print(resource)


if __name__ == "__main__":
    main()
