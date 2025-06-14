# MongoDB Log Analyzer

A Go-based tool for analyzing MongoDB slow query logs and generating comprehensive performance reports.

## Build

```bash
go build -o analyzer
```

## Usage

```bash
./analyzer -i <input_file> -o <output_directory> -s <chunk_size>
```

### Required Flags

- `-i` - Path to the input MongoDB log file (JSON format)
- `-o` - Path to the output directory for generated CSV reports
- `-s` - Chunk size for processing (minimum 100 records)

## Output Files

The tool generates the following CSV reports:

- `*_logs_*.csv` - Raw log entries with structured data
- `*_commands_*.csv` - Slow query commands with execution details
- `*_collscans_*.csv` - Collection scan operations (COLLSCAN queries)
- `*_query_prof.csv` - Query performance profile grouped by query hash
- `*_collection_prof.csv` - Collection performance profile grouped by namespace

## Features

- Processes large log files in configurable chunks
- Extracts slow query performance metrics
- Identifies collection scan operations
- Calculates query execution statistics and ratios
- Groups performance data by query hash and collection
- Handles multiple output files for very large datasets

## Input Format

Expects a MongoDB log file

## Performance Metrics

Reports include:
- Query duration and latency statistics (min/max/average)
- Document examination ratios
- Response size and throughput calculations
- Query frequency and percentage distribution
- Collection scan identification and tracking