/* Construction of the Sample Corpus for Each Concat Level */

/* Concat Level 1: */

  --Stage 1: Select the QG Answer Passages
    --create table `prod_datasets.test_corpus_1_rand_sample`
    select query_id, cast(doc_id as string) as doc_id, title, text, corpus_id_array, questions
    from staging.nq_train_documents_1_qg_25_beam;

  --Stage 2: Sample Answer Document Passages (five from each)
    --insert into `prod_datasets.test_corpus_1_rand_sample`
    select NULL as query_id, cast(doc_id as string) as doc_id,
      title, text, cast(corpus_id_array as string) as corpus_id_array, NULL as questions
    from (
    select A.doc_id, A.title, A.text, corpus_id_array, ROW_NUMBER() OVER(PARTITION BY doc_id ORDER BY RAND()) AS pos
      from (
        select doc_id, title, corpus_id as corpus_id_array, text, row_number() over(partition by A.doc_id, A.title, A.text
            order by cast(substr(corpus_id, 4) as INT64)) as dupes,
        from beir_nq_train.train_document_id A
        where doc_id in (select DISTINCT doc_id from beir_nq_train.train_answer_lookup A
          inner join `beir_nq_train.train_document_id` B on A.corpus_id = B.corpus_id
          inner join `beir_nq_train.train_query_lookup` C on A.query_id = C.query_id
          where C.query_id in (select distinct query_id from `staging.nq_train_documents_3_qg_50`))
      ) A
      where dupes = 1 and corpus_id_array not in (select corpus_id from `beir_nq_train.train_answer_lookup`)
    ) B
    where pos <= 5;

  --Stage 3: Randomly Sample from Wikipedia Corpus
    --insert into `prod_datasets.test_corpus_1_rand_sample`
    select NULL as query_id, doc_id, title, text,
      cast(corpus_id_array as string) as corpus_id_array, NULL as questions
    from (
      select A.doc_id, A.title, A.text, corpus_id_array, RAND() as rnd
      from (
        select doc_id, title, id as corpus_id_array, text,
          row_number() over(partition by doc_id, title, text order by cast(substr(doc_id, 9) as INT64)) as dupes
        from `nlp_final_project.wikipedia_dump_id`
        where title not in (select distinct title from beir_nq_train.train_document_id)
      ) A
      where dupes = 1
    ) B
    where rnd < 0.0075;

/* Concat Level 2: */

  --Stage 1: Select the QG Answer Passages
    --create table prod_datasets.test_corpus_2_rand_sample
    select query_id, cast(doc_id as string) as doc_id, title, text, corpus_id_array, questions
    from staging.nq_train_documents_2_qg_25_beam;

  --Stage 2: Sample Answer Document Passages (five from each)
    -- insert into `prod_datasets.test_corpus_2_rand_sample`
    select NULL as query_id, cast(doc_id as string) as doc_id, title, text, corpus_id_array, NULL as questions
    from (
      select A.doc_id, A.title, A.text, A.corpus_id_array, ROW_NUMBER() OVER(PARTITION BY A.doc_id ORDER BY RAND()) AS pos
      from (
        select doc_id, title, corpus_id_array, doc_text as text, row_number() over(partition by A.doc_id, A.title, A.doc_text
          order by cast(substr(A.corpus_id_array[offset(0)], 4) as INT64))  as dupes,
        from `beir_nq_train.stg_nq_train_documents_2` A
        where doc_id in (select DISTINCT doc_id from`staging.nq_train_documents_3_qg_50`)
      ) A
      where dupes = 1 and array_to_string(A.corpus_id_array, ",")
        not in (select array_to_string(corpus_id_array, ",") from `staging.nq_train_documents_3_qg_50`)
    ) B
    where pos <= 5;

  --Stage 3: Randomly Sample from Wikipedia Corpus
    -- insert into `prod_datasets.test_corpus_2_rand_sample`
    select NULL as query_id, doc_id, B.title, B.text, corpus_id_array, NULL as questions
    from (
      select  A.doc_id, A.title, A.doc_text as text, RAND() as rnd
      from (
        select doc_id, title, wiki_id_array, doc_text,
          row_number() over(partition by doc_id, title, doc_text order by cast(substr(doc_id, 9) as INT64)) as dupes
        from `prod_datasets.wikipedia_documents_2`
        where title not in (select distinct title from beir_nq_train.train_document_id)
      ) A
      where dupes = 1
    ) B
    inner join (
      select doc_text, title,
        array(select cast(elem as string) from unnest(wiki_id_array) elem) corpus_id_array
      from prod_datasets.wikipedia_documents_2
    ) C on B.text = C.doc_text and B.title = C.title
    where rnd < 0.0143;

/* Concat Level 3: */

  --Stage 1: Select the QG Answer Passages
    -- insert into `prod_datasets.test_corpus_3_rand_sample`
    select query_id, cast(doc_id as string) as doc_id, title, text, corpus_id_array, questions
    from staging.nq_train_documents_3_qg_25_beam;

  --Stage 2: Sample Answer Document Passages (five from each)
    -- insert into `prod_datasets.test_corpus_3_rand_sample`
    select NULL as query_id, cast(doc_id as string) as doc_id, title, text, corpus_id_array, NULL as questions
    from (
      select A.doc_id, A.title, A.text, A.corpus_id_array, ROW_NUMBER() OVER(PARTITION BY A.doc_id ORDER BY RAND()) AS pos
      from (
        select doc_id, title, corpus_id_array, doc_text as text, row_number() over(partition by A.doc_id, A.title, A.doc_text
            order by cast(substr(A.corpus_id_array[offset(0)], 4) as INT64))  as dupes,
        from `prod_datasets.nq_train_documents_3` A
        where doc_id in (select DISTINCT doc_id from`staging.nq_train_documents_3_qg_50`)
      ) A
      where dupes = 1 and array_to_string(A.corpus_id_array, ",")
        not in (select array_to_string(corpus_id_array, ",") from `staging.nq_train_documents_3_qg_50`)
    ) B
    where pos <= 5;

  --Stage 3: Randomly Sample from Wikipedia Corpus
    -- insert into `prod_datasets.test_corpus_3_rand_sample`
    select NULL as query_id, doc_id, B.title, B.text, corpus_id_array, NULL as questions
    from (
      select  A.doc_id, A.title, A.doc_text as text, RAND() as rnd
      from (
        select doc_id, title, wiki_id_array, doc_text,
          row_number() over(partition by doc_id, title, doc_text order by cast(substr(doc_id, 9) as INT64)) as dupes
        from `prod_datasets.wikipedia_documents_3`
        where title not in (select distinct title from beir_nq_train.train_document_id)
      ) A
      where dupes = 1
    ) B
    inner join (
      select doc_text, title,
        array(select cast(elem as string) from unnest(wiki_id_array) elem) corpus_id_array
      from prod_datasets.wikipedia_documents_3
    ) C on B.text = C.doc_text and B.title = C.title
    where rnd < 0.02;
