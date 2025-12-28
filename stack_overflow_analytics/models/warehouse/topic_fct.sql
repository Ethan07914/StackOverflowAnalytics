select id,
       tag_name as topic,
       month_name,
       unanswered_questions,
       views,
       links
    from {{ ref('int__tag__joined') }}
