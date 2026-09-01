from pathlib import Path
from uuid import uuid4

from config.settings import SMALL_FILE_THRESHOLD_MB


def get_file_size_mb(file_path: Path) -> float:
    """Return file size in megabytes."""
    return file_path.stat().st_size / (1024 * 1024)


def choose_engine(file_path: Path) -> tuple[str, float, str]:
    """
    Choose the processing engine based on file size.

    Returns:
        engine, file_size_mb, reason
    """

    if not file_path.exists():
        raise FileNotFoundError(f"Input file not found: {file_path}")

    file_size_mb = get_file_size_mb(file_path)

    if file_size_mb <= SMALL_FILE_THRESHOLD_MB:
        engine = "python_batch"
        reason = (
            f"File size ({file_size_mb:.2f} MB) is within the "
            f"small-file threshold ({SMALL_FILE_THRESHOLD_MB} MB)."
        )
    else:
        engine = "pyspark"
        reason = (
            f"File size ({file_size_mb:.2f} MB) exceeds the "
            f"small-file threshold ({SMALL_FILE_THRESHOLD_MB} MB)."
        )

    return engine, file_size_mb, reason


def create_run_id() -> str:
    """Create a unique identifier for one pipeline run."""
    return uuid4().hex


def inspect_file(file_path: str) -> dict:
    """
    Inspect the input file and select the processing engine.
    """

    path = Path(file_path)

    engine, file_size_mb, reason = choose_engine(path)
    run_id = create_run_id()

    result = {
        "run_id": run_id,
        "file_path": str(path),
        "file_name": path.name,
        "file_size_mb": round(file_size_mb, 2),
        "threshold_mb": SMALL_FILE_THRESHOLD_MB,
        "engine_used": engine,
        "reason": reason,
    }

    return result


def print_router_result(result: dict) -> None:
    """Print the router decision in a readable format."""

    print("\n" + "=" * 60)
    print("FILE ROUTER")
    print("=" * 60)

    print(f"Run ID       : {result['run_id']}")
    print(f"File         : {result['file_name']}")
    print(f"Size         : {result['file_size_mb']} MB")
    print(f"Threshold    : {result['threshold_mb']} MB")
    print(f"Engine       : {result['engine_used']}")
    print(f"Reason       : {result['reason']}")

    print("=" * 60 + "\n")