/* Missing Article Analysis */

  /*
    Missing Wiki data:
      DPR Wikipedia data contains 3,232,908 distinct titles, while NQ references only 57,330 distinct titles
      across test/train. How many NQ titles are misssing from wiki based on a title match? Note matching on
      titles is potentially inexact.
  */

  -- crude first pass
    -- nq-test is missing ~6% (202/3452)
      select distinct A.title from `beir_nq_test.test_document_lookup` A
        inner join `beir_nq_test.test_answer_lookup` B on A.corpus_id = B.corpus_id
        where A.title not in (select title from `nlp_final_project.nq_wiki_matching_articles`);

  -- nq-train missing ~13% (7100/56227)
      select distinct A.title from `beir_nq_train.train_document_lookup` A
        inner join `beir_nq_train.train_answer_lookup` B on A.corpus_id = B.corpus_id
        where A.title not in (select title from `nlp_final_project.nq_wiki_matching_articles`);

  -- metrics reported in paper
      select A.title from (
        select distinct title from beir_nq_train.train_document_lookup
      ) A
        left join `nlp_final_project.wikipedia_dump` B
          on REGEXP_REPLACE(lower(A.title), ' ', '') = REGEXP_REPLACE(lower(B.title), ' ', '')
        where B.title is null;

  -- Specific example (Tom Brady)
      -- show query in the Sachan Paper dataset
      select * from `nlp_final_project.nq-train`
        where question = "who had the most wins in the nfl";
      -- show query in BEIR with article title
      select * from `beir_nq_train.train_query_lookup` A
        inner join `beir_nq_train.train_answer_lookup` B on B.query_id = A.query_id
        inner join `beir_nq_train.train_document_lookup` C on B.corpus_id = C.corpus_id
        where A.text = "who had the most wins in the nfl";
      select question_text, document_text, answer, document_url, annotations from `nlp_final_project.sample-nq` A
        inner join `calcium-vial-368801.nlp_final_project.nq-train` B on A.question_text = B.question;
      -- show article with same title not in DPR Wikipedia data (also used finer keyword grep searches)
      select distinct title from `nlp_final_project.wikipedia_dump` A
        where A.title like "%National Football League%" or A.title like "%NFL%";