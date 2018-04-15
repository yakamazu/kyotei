delete from
    public.race_result_odds
where
    race_date
   in
    (
     select
         race_date
     from
         public.wk_race_result_odds
    )
;
