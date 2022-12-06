from datasets import load_dataset
import datasets

## this is the simplified format, but is missing the tokenization
# dataset = load_dataset("json", data_files="/home/nimbus/Documents/NLP/data/NQ/v1.0-simplified_simplified-nq-train.jsonl/simplified-nq-train.jsonl")

## load a training sample from original format
# dataset_tr = load_dataset("json", data_files="/home/nimbus/Documents/NLP/data/v1.0/train/train_00_n_1500.jsonl")

# to access one sample entry (a dictionary)
# dataset['train'][0].keys()


## try the parquest conversion
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# json_file = "./data/NQ/chunks_n_10000/sample_al.jsonl"
json_file = "./data/NQ/v1.0-simplified_simplified-nq-train/sample.json"
parquet_file = "sample-nq.parquet"
# chunksize = 100_000
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