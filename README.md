## Data files
The TPC-H `.tbl` dataset files are not included due to size limits.
Generate them using the official TPC-H tools:
https://www.tpc.org/tpch/

# RDBMS to NoSQL Migration — CLDA Implementation

Implementation and benchmarking of three database migration strategies
based on Yoo, Lee & Jeon (JISE 2018).

## Methods compared
- Strict Normalization
- Table-Level Denormalization (BFS)
- Column-Level Denormalization with Atomicity (CLDA)

## Tech stack
Python · MongoDB · Pandas · TPC-H Benchmark

## Results
CLDA achieved 2.2× faster query execution with only 1.5× storage overhead.
