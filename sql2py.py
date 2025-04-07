import pandas as pd
import sys
import traceback
from collections import defaultdict

def main():
    if len(sys.argv) != 3:
        print("Usage: python3 sql2py.py <inspections.csv> <chunk_size>")
        sys.exit(1)

    file_path = sys.argv[1]

    try:
        chunk_size = int(sys.argv[2])
    except ValueError:
        print("Error: Chunk size must be an integer.")
        sys.exit(1)

    # Initialize data structures
    score_sums = defaultdict(float)
    count_dict = defaultdict(int)

    try:
        for chunk in pd.read_csv(file_path, chunksize=chunk_size):
            # TODO: Filter chunk
            filtered = chunk[
                (chunk['facility_name'].str.contains("coffee", case=False, na=False)) &
                (chunk['program_status'].str.lower() == 'active')
            ]
            # TODO: Group and aggregate
            grouped = filtered.groupby('facility_name')['score'].agg(['sum', 'count'])
            for facility, row in grouped.iterrows():
                score_sums[facility] += row['sum']
                count_dict[facility] += row['count']
            # TODO: Update aggregates
            pass

    except Exception:
        traceback.print_exc()
        sys.exit(1)

    # TODO: Compute average scores

    results = []
    for facility in sorted(score_sums):
        count = count_dict[facility]
        if count >= 10:
            avg_score = score_sums[facility] / count
            results.append((facility, avg_score))

    # TODO: Output results

    results.sort(key=lambda x: x[0])
    for facility, avg in results:
        print(f"{facility},{avg:.6f}")

if __name__ == "__main__":
    main()
