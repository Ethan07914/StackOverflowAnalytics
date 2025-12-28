with questions as (
    select *
       from {{ ref('int__posts_questions__filtered') }}),

links as (
   select post_id, count(post_id) as link_count
       from {{ ref('stg_bigquery__post_links') }}
          where EXTRACT(YEAR FROM creation_date) = 2019
   group by post_id),

final as (
   select questions.*, links.link_count
       from questions
       left join links on questions.id = links.post_id)

select * from final