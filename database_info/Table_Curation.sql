/* Table Curation */

  /*
    Contains table curation logic that adds a document id by title and collapses consecutive passages together.
  */

  --Raw tables are missing a document id so we add it.
    --create table `nlp_final_project.wikipedia_dump_id` as
    select concat("wikidoc_", DENSE_RANK() OVER (order by title)) as doc_id, id,
      CASE WHEN title is NULL THEN "NaN" ELSE title end as title,
      CASE when substr(text, 0,length(title)) = title THEN substr(text, length(title)+2) else text end as text
    from nlp_final_project.wikipedia_dump;

  --An example for concat level 2. After concatenation we apply the duplicate removal logic.
  -- create table `prod_datasets.wikipedia_documents_2` as
    select doc_id, wiki_id_array, title, doc_text
    from (
      select ROW_NUMBER() Over (partition by A.doc_id order by A.id) as sub_grp
            , A.doc_id
            , A.title
            , A.id as wiki_id
            , CASE
                WHEN B.id is null THEN [A.id]
                ELSE [A.id, B.id]
              END as wiki_id_array
            , CASE
                WHEN B.id is null THEN REPLACE(REPLACE(A.text, CHR(13), ' '), CHR(10), ' ')
                ELSE concat(REPLACE(REPLACE(A.text, CHR(13), ' '), CHR(10), ' '), ' ',
                  REPLACE(REPLACE(B.text, CHR(13), ' '), CHR(10), ' '))
              END AS doc_text
      from `nlp_final_project.wikipedia_dump_id` A
      left join `nlp_final_project.wikipedia_dump_id` B on A.doc_id = B.doc_id and A.id = B.id - 1
    ) AA
    where mod(sub_grp, 2) = 1;

  --A level 3 example
    --create table prod_datasets.nq_train_documents_3 as
    select doc_id, corpus_id_array, title, doc_text
      from (
        select ROW_NUMBER() Over (partition by A.doc_id order by cast(substr(A.corpus_id, 4) as INT64)) as sub_grp
        , A.doc_id, A.title, A.corpus_id as train_corpus_id
        , CASE
          WHEN C.corpus_id is null and B.corpus_id is null THEN [A.corpus_id]
          WHEN C.corpus_id is null THEN [A.corpus_id, B.corpus_id]
          ELSE [A.corpus_id, B.corpus_id, C.corpus_id]
        END as corpus_id_array
        , CASE
          WHEN C.corpus_id is null and B.corpus_id is null THEN REPLACE(REPLACE(A.text, CHR(13), ' '), CHR(10), ' ')
          WHEN C.corpus_id is null THEN concat(REPLACE(REPLACE(A.text, CHR(13), ' '), CHR(10), ' '),
            ' ', REPLACE(REPLACE(B.text, CHR(13), ' '), CHR(10), ' '))
          ELSE
          concat(REPLACE(REPLACE(A.text, CHR(13), ' '), CHR(10), ' '), ' '
          ,REPLACE(REPLACE(B.text, CHR(13), ' '), CHR(10), ' '), ' ',
            REPLACE(REPLACE(C.text, CHR(13), ' '), CHR(10), ' '))
          END AS doc_text
      from `beir_nq_train.train_document_id` A
      left join `beir_nq_train.train_document_id` B on A.doc_id = B.doc_id and
      cast(substr(A.corpus_id, 4) as INT64) = cast(substr(B.corpus_id, 4) as INT64) - 1
      left join `beir_nq_train.train_document_id` C on B.doc_id = C.doc_id and
      cast(substr(B.corpus_id, 4) as INT64) = cast(substr(C.corpus_id, 4) as INT64) - 1
      ) AA
      where mod(sub_grp, 3) = 1 order by AA.doc_id, AA.train_corpus_id;