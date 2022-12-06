import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# json_file = "./data/NQ/chunks_n_10000/sample_al.jsonl"
json_file = "./data/NQ/v1.0-simplified_simplified-nq-train/sample.json"
parquet_file = "sample-nq.parquet"
chunksize = 20000

json_stream = pd.read_json(json_file, chunksize=chunksize, lines=True)

for i, chunk in enumerate(json_stream):
    print("Chunk", i)
## there are large ids in the jsons that are causing issues
    # chunk.drop('example_id', axis=1, inplace=True)
    # chunk.drop('annotations', axis=1, inplace=True)
    chunk.annotations.apply(lambda x: x[0].pop("annotation_id"))
    if i == 0:
        parquet_schema = pa.Table.from_pandas(df=chunk).schema
        parquet_writer = pq.ParquetWriter(parquet_file, parquet_schema, compression='snappy')
    table = pa.Table.from_pandas(chunk, schema=parquet_schema)
    parquet_writer.write_table(table)

parquet_writer.close()