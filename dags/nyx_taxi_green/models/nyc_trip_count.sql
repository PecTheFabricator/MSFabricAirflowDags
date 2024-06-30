with new_york_taxis as (
       select * from nyctlc
   ),
   final as (
     SELECT
       vendorID,
       CAST(lpepPickupDatetime AS DATE) AS trip_date,
       COUNT(*) AS trip_count
     FROM
         [contoso-data-warehouse].[dbo].[nyctlc]
     GROUP BY
         vendorID,
         CAST(lpepPickupDatetime AS DATE)
     ORDER BY
         vendorID,
         trip_date;
   )
   select * from final
