#!/usr/bin/env python3
"""Convert CSV files to Parquet format."""

import pandas as pd
from pathlib import Path
import argparse


def convert_csv_to_parquet(csv_path: str, output_dir: str = None) -> None:
    """Convert a CSV file to Parquet format.
    
    Args:
        csv_path: Path to the CSV file
        output_dir: Directory to save the Parquet file (default: same as CSV)
    """
    csv_path = Path(csv_path)
    
    # Determine output path
    if output_dir:
        output_path = Path(output_dir) / f"{csv_path.stem}.parquet"
    else:
        output_path = csv_path.with_suffix('.parquet')
    
    # Create output directory if it doesn't exist
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    print(f"Converting {csv_path} to {output_path}")
    
    # Read CSV file
    df = pd.read_csv(csv_path)
    
    # Save as Parquet
    df.to_parquet(output_path, index=False)
    
    # Print file size comparison
    csv_size = csv_path.stat().st_size / (1024 * 1024)  # Size in MB
    parquet_size = output_path.stat().st_size / (1024 * 1024)  # Size in MB
    
    print(f"\nConversion complete!")
    print(f"Original CSV size: {csv_size:.2f} MB")
    print(f"Parquet size: {parquet_size:.2f} MB")
    print(f"Compression ratio: {csv_size/parquet_size:.2f}x")


def main():
    parser = argparse.ArgumentParser(description='Convert CSV files to Parquet format')
    parser.add_argument('csv_path', help='Path to the CSV file or directory containing CSV files')
    parser.add_argument('--output-dir', help='Directory to save Parquet files (default: same as input)')
    
    args = parser.parse_args()
    
    csv_path = Path(args.csv_path)
    
    if csv_path.is_file():
        # Convert single file
        convert_csv_to_parquet(str(csv_path), args.output_dir)
    elif csv_path.is_dir():
        # Convert all CSV files in directory
        for csv_file in csv_path.glob('**/*.csv'):
            convert_csv_to_parquet(str(csv_file), args.output_dir)
    else:
        print(f"Error: {csv_path} does not exist")


if __name__ == '__main__':
    main() 