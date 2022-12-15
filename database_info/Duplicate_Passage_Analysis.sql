/* Duplicate Passage Analysis */

  --To find and remove duplicates we a variation of the following query that takes the min corpus_id from the
  --concat level 2 base table
    select doc_id, title, text, corpus_id_array
    from (
        select A.doc_id, A.title, A.doc_text as text,
            A.corpus_id_array,
            row_number() over(partition by A.doc_id, A.title,
                A.doc_text order by
                    cast(substr(A.corpus_id_array[offset(0)], 4)
                        as INT64)) as dupes
        from `beir_nq_train.stg_nq_train_documents_2` A
    ) A
    where dupes = 1;