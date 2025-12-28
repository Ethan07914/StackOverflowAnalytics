with posts as (
  select *
      from {{ ref('int__post__joined')
      }}),

posts_with_tags as (
 select posts.*,
        tag
     from posts,
     UNNEST(SPLIT(posts.tags, '|')) AS tag
 ),

tags as (
   select *
      from {{ ref('stg_bigquery__tags') }}),

final as (
   select
          CONCAT(tags.tag_name, '_', pwt.month_name) as id,
          tags.tag_name,
          pwt.month_name,
          count(pwt.id) as unanswered_questions,
          sum(pwt.view_count) as views,
          sum(pwt.link_count) as links
              from tags
              inner join posts_with_tags as pwt
              on tags.tag_name = pwt.tag
  group by id, tags.tag_name, pwt.month_name)


select * from final
 order by unanswered_questions desc
