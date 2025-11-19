from pathlib import Path

from pathling import PathlingContext


def main():
    pc = PathlingContext.create()

    path = Path("repeat-deltas")
    if path.exists():
        import shutil

        shutil.rmtree(path)

    df = pc.read.ndjson("min-export")

    view = df.view(
        resource="Communication",
        select=[{"column": [{"path": "getResourceKey()", "name": "resource_id"}]}],
    )

    statuses: list[tuple[int, str]] = []
    for i in range(5):
        df.write.delta("repeat-deltas", save_mode="merge")

        # Get the size of the folder and print it
        import os

        folder_size = sum(
            os.path.getsize(os.path.join(dirpath, filename))
            for dirpath, dirnames, filenames in os.walk("repeat-deltas")
            for filename in filenames
        )
        size = f"{round(folder_size / 1024 / 1024, 0)} MiB"

        count = (
            pc.read.delta("repeat-deltas")
            .view(
                resource="Communication",
                select=[
                    {"column": [{"path": "getResourceKey()", "name": "resource_id"}]}
                ],
            )
            .count()
        )
        statuses.append((count, size))
        print(statuses)

    print(f"Final statuses: {statuses}")


if __name__ == "__main__":
    main()
