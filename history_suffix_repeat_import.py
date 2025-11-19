from pathlib import Path

import polars
from pathling import DataSource, PathlingContext


def get_count(df: DataSource) -> int:
    view = df.view(
        resource="Communication",
        select=[{"column": [{"path": "getResourceKey()", "name": "resource_id"}]}],
    )
    return view.count()


def main_delta():
    pc = PathlingContext.create()

    if Path("min-export-delta").exists():
        import shutil

        shutil.rmtree("min-export-delta")

    df = pc.read.ndjson("min-export")
    df.write.delta("min-export-delta", save_mode="merge")
    delta = pc.read.delta("min-export-delta")

    print(get_count(delta))

    df2 = pc.read.ndjson("min-export-v2")
    df2.write.delta("min-export-delta", save_mode="merge")
    delta = pc.read.delta("min-export-delta")
    print(get_count(delta))

    pl_df = polars.read_delta("min-export-delta/Communication.parquet")
    pass


def main_parquet():
    pc = PathlingContext.create()

    if Path("min-export-delta").exists():
        import shutil

        shutil.rmtree("min-export-delta")

    df = pc.read.ndjson("min-export")
    df.write.parquet("min-export-parquet")

    pl_df = polars.read_parquet("min-export-parquet/Communication.parquet/*.parquet")
    pass


if __name__ == "__main__":
    main_parquet()
