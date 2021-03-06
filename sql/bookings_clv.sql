WITH BOOKINGS AS(
SELECT B.SRC_BOOKING_ID,
        B.CLIENT_ID,
        T.TIME_VALUE AS PICKUP_TIME,
        D.DATE_DT AS PICKUP_DATE,
        U.SRC_USER_ID
    FROM DWH.WC_BOOKING_F B,
        DWH.WC_DATE_D D,
        DWH.WC_TIME_D T,
        DWH.WC_USER_D U
WHERE B.CLIENT_ID IN {{params.dwh_client_ids}}
and B.PICKUP_DATE_ID=D.DATE_ID
AND B.PICKUP_TIME_ID=T.TIME_ID
and B.USER_ID=U.USER_ID
),
PAYMENTS AS(
SELECT SRC_BOOKING_ID, PRICE_TOTAL
FROM DWH.WC_PAYMENT_F
WHERE CLIENT_ID IN {{params.dwh_client_ids}}
)
select DISTINCT BOOKINGS.SRC_USER_ID,
                BOOKINGS.PICKUP_DATE,
                BOOKINGS.PICKUP_TIME,
                PAYMENTS.PRICE_TOTAL,
                BOOKINGS.SRC_BOOKING_ID
from BOOKINGS
INNER JOIN PAYMENTS
on BOOKINGS.SRC_BOOKING_ID=PAYMENTS.SRC_BOOKING_ID
    where PAYMENTS.PRICE_TOTAL > 0
    and PICKUP_DATE between TO_DATE ('{{params.bookings_start_date}}', 'yyyy/mm/dd')
    AND TO_DATE ('{{params.bookings_end_date}}', 'yyyy/mm/dd')