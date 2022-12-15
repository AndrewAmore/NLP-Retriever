/* Lookup Batch Table Construction */

  /*
    To reduce runtime of question generation, we create lookup tables that function like a queue to
    enable parallel processing across multiple GPUs. When a session finishes processing a batch of records
    it deletes the batch from the lookup table, essentially removing it from the queue.
  */

  --here is an example of a lookup table from the level 2 random sample
    --create table testing.stg_test_corpus_2_batch_loading
    select query_id, doc_id, title, text from `prod_datasets.test_corpus_2_rand_sample`
    where query_id is null;


  /*
    When a batching process finishes, results are appended to a new table and we need to update the
    results.
  */
    update `prod_datasets.test_corpus_1_rand_sample` A
      set A.questions = B.questions
    from testing.test_corpus_1_qg_25_beam B
    where B.text = A.text and A.title = B.title and A.doc_id = B.doc_id;