with activity_table AS (
    select
      TO_TIMESTAMP(TO_CHAR(to_date(DATE_ID, 'YYYYMMDD'), 'YYYY-MM-DD') || ' ' ||
                   TO_CHAR(TIME_ID, 'fm00G00G00', 'NLS_NUMERIC_CHARACTERS=''.:'''),
                   'YYYY-MM-DD HH24:MI:SS')          as datetime,
      LAG(SAT.SAT_REFERENCE, 3, -1)
      OVER (
        PARTITION BY DRIVER_ID
        ORDER BY DATE_ID, TIME_ID, SRC_ACTIVITY_ID ) AS action_prev_three,
      LAG(SAT.SAT_REFERENCE, 2, -1)
      OVER (
        PARTITION BY DRIVER_ID
        ORDER BY DATE_ID, TIME_ID, SRC_ACTIVITY_ID ) AS action_prev_two,
      LAG(SAT.SAT_REFERENCE, 1, -1)
      OVER (
        PARTITION BY DRIVER_ID
        ORDER BY DATE_ID, TIME_ID, SRC_ACTIVITY_ID ) AS action_prev,
      SAT.SAT_REFERENCE                              as action,
      LEAD(SAT.SAT_REFERENCE, 1, -1)
      OVER (
        PARTITION BY DRIVER_ID
        ORDER BY DATE_ID, TIME_ID, SRC_ACTIVITY_ID ) AS action_next,
      LEAD(SAT.SAT_REFERENCE, 2, -1)
      OVER (
        PARTITION BY DRIVER_ID
        ORDER BY DATE_ID, TIME_ID, SRC_ACTIVITY_ID ) AS action_next_two,
      LEAD(SAT.SAT_REFERENCE, 3, -1)
      OVER (
        PARTITION BY DRIVER_ID
        ORDER BY DATE_ID, TIME_ID, SRC_ACTIVITY_ID ) AS action_next_three,
      DR_ACT.*
    from fnd.WC_DISP_DRIVERACTIVITY_FS DR_ACT,
      FND.WC_SAT_DS SAT
    where CLIENT_ID in {{params.dwh_client_ids}}
          AND DR_ACT.DATE_ID BETWEEN {{params.bookings_start_date}} AND {{params.bookings_end_date}}
          AND DR_ACT.ACTION_ID = SAT.SAT_ID
    order by DATE_ID, TIME_ID, SRC_ACTIVITY_ID),
 date_added as (
select
  LAG(datetime, 1, DATE '2020-01-01')
  OVER (
    PARTITION BY DRIVER_ID
    ORDER BY DATE_ID, TIME_ID, SRC_ACTIVITY_ID ) AS previous_action_datetime,
  activity_table.*
from activity_table)
select
    ((datetime + 0) - (previous_action_datetime + 0)) * 24 * 60 * 60 AS time_delta,
    date_added.*
from date_added