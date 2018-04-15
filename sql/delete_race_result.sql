delete from
    public.race_result
where
    race_date
   in
    (
     select
         race_date
     from
         public.wk_race_result
    )
;
