(
    SELECT 'from_table1_not_in_table2' AS source, t1.*
    FROM {{ref('helen_daily_issue_dashboard_test')}} AS t1
    EXCEPT
    SELECT 'from_table1_not_in_table2' AS source, t2.*
    FROM {{ref('siddhant_daily_issue_dashboard_test')}} AS t2
)
UNION ALL
(
    SELECT 'from_table2_not_in_table1' AS source, t2.*
    FROM {{ref('siddhant_daily_issue_dashboard_test')}} AS t2
    EXCEPT
    SELECT 'from_table2_not_in_table1' AS source, t1.*
    FROM {{ref('helen_daily_issue_dashboard_test')}} AS t1
)
