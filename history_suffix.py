from pathling import PathlingContext


def main():
    pc = PathlingContext.create()

    df = pc.read.ndjson("min-export")

    view = df.view(
        resource="Communication",
        select=[{"column": [{"path": "getResourceKey()", "name": "resource_id"}]}],
    )
    print(view.head(10))


if __name__ == "__main__":
    main()
