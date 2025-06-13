# Analyzer

A MongoDB log analysis tool that processes MongoDB slow query logs and generates performance reports.

## Build

```bash
go build -o analyzer
```

## Usage

```bash
./analyzer -i <input_file> -o <output_directory>
```

### Required Flags

- `-i` - Path to the input MongoDB log file (JSON format)
- `-o` - Path to the output directory for generated CSV reports

## Output Files

The tool generates the following CSV reports:

- `*_logs.csv` - Raw log entries with structured data
- `*_commands.csv` - Slow query commands with execution details
- `*_query_prof.csv` - Query performance profile grouped by query hash
- `*_collection_prof.csv` - Collection performance profile grouped by namespace

## Features

- Processes large log files in chunks (100,000 records at a time)
- Extracts slow query performance metrics
- Calculates query execution statistics and ratios
- Groups performance data by query hash and collection
- Handles multiple output files for very large datasets

## Input Format

Expects MongoDB log files in JSON format with slow query entries containing:
- Query execution time
- Documents examined/returned
- Response size
- Query hash and namespace information

## Performance Metrics

Reports include:
- Query duration and latency statistics
- Document examination ratios
- Response size and throughput
- Query frequency and percentage distribution