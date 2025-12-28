{% set month_dict = {
   1: 'January',
   2: 'February',
   3: 'March',
   4: 'April',
   5: 'May',
   6: 'June',
   7: 'July',
   8: 'August',
   9: 'September',
   10: 'October',
   11: 'November',
   12: 'December']} %}

with most_recent_year as (
    select *
        from {{ ref('stg_bigquery_posts_questions') }}
            where EXTRACT(YEAR from creation_date) = 2019), -- Would instead use = EXTRACT(YEAR FROM current_year()) if the data was up to date

final as (
    select *,
        CASE EXTRACT(MONTH FROM creation_date)
            {%- for month, month_name in month_dict.items() %}
            when {{ month }} THEN '{{ month_name }}'
            {% endfor %}
        END AS month_name
            from most_recent_year)

select * from final