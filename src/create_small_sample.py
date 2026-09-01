import argparse
import csv
from pathlib import Path


def create_sample(input_path: str, output_path: str, rows: int) -> None:
    input_file = Path(input_path)
    output_file = Path(output_path)

    if not input_file.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")

    if rows <= 0:
        raise ValueError("Number of rows must be greater than 0.")

    output_file.parent.mkdir(parents=True, exist_ok=True)

    with (
        input_file.open("r", encoding="utf-8-sig", newline="") as source,
        output_file.open("w", encoding="utf-8", newline="") as target,
    ):
        reader = csv.reader(source)
        writer = csv.writer(target)

        header = next(reader)
        writer.writerow(header)

        rows_written = 0

        for row in reader:
            writer.writerow(row)
            rows_written += 1

            if rows_written >= rows:
                break

    print("Sample creation completed.")
    print(f"Input file:     {input_file}")
    print(f"Output file:    {output_file}")
    print(f"Rows requested: {rows}")
    print(f"Rows written:   {rows_written}")


def main():
    parser = argparse.ArgumentParser(
        description="Create a reproducible sample from a large CSV file."
    )

    parser.add_argument(
        "--input",
        required=True,
        help="Path to the original CSV file.",
    )

    parser.add_argument(
        "--output",
        default="data/samples/orders_sample.csv",
        help="Path for the generated sample.",
    )

    parser.add_argument(
        "--rows",
        type=int,
        default=100_000,
        help="Number of data rows to extract.",
    )

    args = parser.parse_args()

    create_sample(
        input_path=args.input,
        output_path=args.output,
        rows=args.rows,
    )


if __name__ == "__main__":
    main()