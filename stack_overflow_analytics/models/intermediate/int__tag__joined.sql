with posts as (
  select *
      from {{ ref('int__post__joined')}}),

tags as (
   select *
      from {{ ref('stg_bigquery_tags') }}),

final as (
   select
          CONCAT(tags.tag_name, '_', posts.month_name) as id,
          tags.tag_name,
          posts.month_name,
          count(posts.id) as questions,
          sum(posts.answer_count) as answers,
          sum(posts.view_count) as views,
          sum(posts.link_count) as links
              from tags
              left join posts
              on tags.tag_name in UNNEST(SPLIT(posts.tags, '|'))
  group by id)


select * from final
 order by questions desc, answers asc