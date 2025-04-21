# Finance Research Project Makefile

# Default Python interpreter using uv
PYTHON := uv run

# Help target
.PHONY: help
help:
	@echo "Finance Research Project"
	@echo ""
	@echo "Available commands:"
	@echo "  make convert-csv        - Convert CSV files to Parquet format"
	@echo ""
	@echo "CSV to Parquet Conversion:"
	@echo "  make convert-csv FILE=path/to/file.csv [OUTPUT=path/to/output]"
	@echo "  make convert-all-csv [INPUT=path/to/input/dir] [OUTPUT=path/to/output/dir]"
	@echo ""
	@echo "Examples:"
	@echo "  make convert-csv FILE=data.csv OUTPUT=output/"
	@echo "  make convert-all-csv INPUT=data/ OUTPUT=output/"
	@echo ""
	@echo "Options:"
	@echo "  FILE    - Path to a single CSV file to convert"
	@echo "  OUTPUT  - Directory to save the Parquet file (default: same as input)"
	@echo "  INPUT   - Directory containing CSV files to convert"
	@echo "  PYTHON  - Python interpreter to use (default: uv run)"


# Convert a single CSV file to Parquet
.PHONY: convert-csv
convert-csv:
	@echo "Converting $(FILE) to Parquet format..."
	$(PYTHON) common/utils/data_conversion/csv_to_parquet.py $(FILE) $(if $(OUTPUT),--output-dir $(OUTPUT),)

# Convert all CSV files in a directory
.PHONY: convert-all-csv
convert-all-csv:
	echo "Converting all CSV files in $(INPUT)..."; \
	$(PYTHON) common/utils/data_conversion/csv_to_parquet.py $(INPUT) $(if $(OUTPUT),--output-dir $(OUTPUT),); \
