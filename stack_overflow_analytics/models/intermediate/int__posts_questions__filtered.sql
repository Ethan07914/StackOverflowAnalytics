{% set months = [
   'January',
   'February',
   'March',
   'April',
   'May',
   'June',
   'July',
   'August',
   'September',
   'October',
   'November',
   'December'] %}

with unanswered_questions as (
    select *
        from {{ ref('stg_bigquery__posts_questions') }}
            where (answer_count is null or answer_count = 0)),


most_recent_year as (
    select *
        from unanswered_questions
            where EXTRACT(YEAR from creation_date) = 2019), -- Would instead use = EXTRACT(YEAR FROM current_year()) if the data was up to date

final as (
    select *,
        CASE EXTRACT(MONTH FROM creation_date)
            {%- for month in range(12) %}
            when {{ month + 1 }} THEN '{{ months[month] }}'
            {% endfor %}
        END AS month_name
            from most_recent_year)

select * from final