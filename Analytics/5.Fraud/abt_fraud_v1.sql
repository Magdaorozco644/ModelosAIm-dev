-- Creamos tablas auxiliares para generar la abt
-- Creamos tabla universo base (desde noviembre en adelante)
create table analytics.source_fraud_m18 as (
    SELECT
        trim(trans.id_branch) AS id_branch,
        try(cast(trim(trans.id_receiver) as integer)) as id_receiver,
        try(cast(trans.DATE_RECEIVER as timestamp)) AS DATE_RECEIVER,
        trim(trans.id_location) as id_location,
        id_payer,
        try(cast(trim(trans.id_sender_global) as integer)) as id_sender_global,
        net_amount_receiver,
        id_payout,
        status,
        was_fraud,
        receiver_transaction_count
        receiver_date_first_transaction,
        receiver_date_last_transaction,
        id_country_receiver_claim,
        id_state_receiver_claim,
        id_state,
        branch_working_days,
        sender_sending_days,
        sender_days_to_last_transaction,
        id_country,
        fraud_classification,
        sender_minutes_since_last_transaction,
        branch_minutes_since_last_transaction
        sender_days_since_last_transaction
    FROM
        viamericas.source_fraud trans
    where try(cast(trans.DATE_RECEIVER as timestamp)) >= cast('2023-01-01' as timestamp)--date_add( 'month', -18, current_date)
);


-- branch_trans_4min
create table analytics.branch_trans_40min_cte_lg as (
    select trans.id_branch, trans.id_receiver, count(rc.date_receiver) as branch_trans_40min
    from
    analytics.source_fraud_m18 trans left join
        viamericas.receiver rc
    on trans.id_branch = trim(rc.id_branch)
    and rc.date_receiver between date_add('minute',-40, trans.DATE_RECEIVER ) and  trans.DATE_RECEIVER
    group by trans.id_branch, trans.id_receiver
);

-- branch_trans_10min
create table analytics.branch_trans_10min_cte_lg as (
    select trans.id_branch,  trans.id_receiver,  count(rc.date_receiver) as branch_trans_10min
    from
    analytics.source_fraud_m18 trans left join
        viamericas.receiver rc
    on trans.id_branch = trim(rc.id_branch)
    and rc.date_receiver between date_add('minute',-10, trans.DATE_RECEIVER) and trans.DATE_RECEIVER
    group by trans.id_branch, trans.id_receiver
 );

-- Creamos por cada año una tabla auxiliar y luego unimos
create table analytics.branch_trans_3m_distinct_cte_2020_lg as (
    select trans.id_branch, trans.id_receiver, count(rc.date_receiver) as branch_trans_3m, count(distinct(date_trunc('day', rc.date_receiver))) count_date_receiver_distinct
    from
    analytics.source_fraud_m18 trans left join
        viamericas.receiver rc
    on trans.id_branch = trim(rc.id_branch)
    and rc.date_receiver between date_add('month',-3, trans.DATE_RECEIVER) and  trans.DATE_RECEIVER
    where (rc.date_receiver between cast('2020-01-01' as timestamp) and cast('2020-12-31' as timestamp))
    group by trans.id_branch, trans.id_receiver
);
create table analytics.branch_trans_3m_distinct_cte_2021_lg as (
    select trans.id_branch, trans.id_receiver, count(rc.date_receiver) as branch_trans_3m, count(distinct(date_trunc('day', rc.date_receiver))) count_date_receiver_distinct
    from
    analytics.source_fraud_m18 trans left join
        viamericas.receiver rc
    on trans.id_branch = trim(rc.id_branch)
    and rc.date_receiver between date_add('month',-3, trans.DATE_RECEIVER) and  trans.DATE_RECEIVER
    where (rc.date_receiver between cast('2021-01-01' as timestamp) and cast('2021-12-31' as timestamp))
    group by trans.id_branch, trans.id_receiver
);
create table analytics.branch_trans_3m_distinct_cte_2022_lg as (
    select trans.id_branch, trans.id_receiver, count(rc.date_receiver) as branch_trans_3m, count(distinct(date_trunc('day', rc.date_receiver))) count_date_receiver_distinct
    from
    analytics.source_fraud_m18 trans left join
        viamericas.receiver rc
    on trans.id_branch = trim(rc.id_branch)
    and rc.date_receiver between date_add('month',-3, trans.DATE_RECEIVER) and  trans.DATE_RECEIVER
    where (rc.date_receiver between cast('2022-01-01' as timestamp) and cast('2022-12-31' as timestamp))
    group by trans.id_branch, trans.id_receiver
);
create table analytics.branch_trans_3m_distinct_cte_2023_01_lg as (
    select trans.id_branch, trans.id_receiver, count(rc.date_receiver) as branch_trans_3m, count(distinct(date_trunc('day', rc.date_receiver))) count_date_receiver_distinct
    from
    analytics.source_fraud_m18 trans left join
        viamericas.receiver rc
    on trans.id_branch = trim(rc.id_branch)
    and rc.date_receiver between date_add('month',-3, trans.DATE_RECEIVER) and  trans.DATE_RECEIVER
    where (rc.date_receiver between cast('2023-01-01' as timestamp) and cast('2023-06-30' as timestamp))
    group by trans.id_branch, trans.id_receiver
);
create table analytics.branch_trans_3m_distinct_cte_2023_02_lg as (
    select trans.id_branch, trans.id_receiver, count(rc.date_receiver) as branch_trans_3m, count(distinct(date_trunc('day', rc.date_receiver))) count_date_receiver_distinct
    from
    analytics.source_fraud_m18 trans left join
        viamericas.receiver rc
    on trans.id_branch = trim(rc.id_branch)
    and rc.date_receiver between date_add('month',-3, trans.DATE_RECEIVER) and  trans.DATE_RECEIVER
    where (rc.date_receiver between cast('2023-07-01' as timestamp) and cast('2023-12-31' as timestamp))
    group by trans.id_branch, trans.id_receiver
);
create table analytics.branch_trans_3m_distinct_cte_2024_lg as (
    select trans.id_branch, trans.id_receiver, count(rc.date_receiver) as branch_trans_3m, count(distinct(date_trunc('day', rc.date_receiver))) count_date_receiver_distinct
    from
    analytics.source_fraud_m18 trans left join
        viamericas.receiver rc
    on trans.id_branch = trim(rc.id_branch)
    and rc.date_receiver between date_add('month',-3, trans.DATE_RECEIVER) and  trans.DATE_RECEIVER
    where (rc.date_receiver between cast('2024-01-01' as timestamp) and cast('2024-12-31' as timestamp))
    group by trans.id_branch, trans.id_receiver
);
-- UNION
-- branch_prom_diario_count_distinct
-- branch_trans_3m
create table analytics.branch_trans_3m_distinct_cte as (
    select t1.id_branch, t1.id_receiver, sum(t1.branch_trans_3m) as branch_trans_3m, sum(t1.count_date_receiver_distinct) as count_date_receiver_distinct
    from (
        select * from analytics.branch_trans_3m_distinct_cte_2020_lg
        union
        select * from analytics.branch_trans_3m_distinct_cte_2021_lg
        union
        select * from analytics.branch_trans_3m_distinct_cte_2022_lg
        union
        select * from analytics.branch_trans_3m_distinct_cte_2023_01_lg
        union
        select * from analytics.branch_trans_3m_distinct_cte_2023_02_lg
        union
        select * from analytics.branch_trans_3m_distinct_cte_2024_lg
    ) t1
    group by t1.id_branch, t1.id_receiver
);



-- cash_pick_up_4min
create table analytics.cash_pick_up_4min_cte as (
    select trans.id_branch as id_branch, trans.id_receiver, count(rc.date_receiver) as cash_pick_up_4min
    from
    analytics.source_fraud_m18 trans left join
        viamericas.receiver rc
    on trans.id_branch = trim(rc.id_branch)
    and rc.date_receiver between date_add('minute',-4, trans.DATE_RECEIVER) and  trans.DATE_RECEIVER
    and rc.mode_pay_receiver in ('M','P','S')
    group by trans.id_branch, trans.id_receiver
);


--------------------------------------------------------------- ABT -----------------------------------------------

create table analytics.abt_fraud as (
    with receiver_fraud_fecha as(
        SELECT
            r.DATE_RECEIVER, r.ID_RECEIVER, r.ID_BRANCH, rf.fraud_type, rf.fraud_classification,
            rf.incident_code, r.id_sender , r.id_country_receiver, r.id_payment, r.id_state_receiver,
            r.id_city_receiver, r.bank_receiver
        FROM
            viamericas.RECEIVER_FRAUD rf
        INNER JOIN
                viamericas.RECEIVER r
        ON rf.ID_BRANCH = r.ID_BRANCH AND rf.ID_RECEIVER = r.ID_RECEIVER
    ),
    branch_and_fecha_last_fraud as (
        select trans.id_branch, trans.id_receiver, count(fr.fraud_type) branch_has_fraud, max(fr.date_receiver)  as fecha_last_fraud_branch
        from
        analytics.source_fraud_m18 trans left join
        receiver_fraud_fecha fr
        on trans.id_branch = trim(fr.id_branch) and fr.date_receiver < trans.DATE_RECEIVER
        and fr.fraud_classification = 'Real'
        group by trans.id_branch, trans.id_receiver
    ) ,
    receiver_has_fraud_cte as (
        select trans.id_receiver, trans.id_branch, count(fr.fraud_type) as receiver_has_fraud
        from
        analytics.source_fraud_m18 trans left join
        receiver_fraud_fecha fr
        on trans.id_receiver = fr.id_receiver and fr.date_receiver < trans.DATE_RECEIVER
        and fr.fraud_classification = 'Real'
        group by trans.id_receiver, trans.id_branch
    ),
    location_nro_fraud_cte as (
        select trans.id_location, trans.id_receiver, trans.id_branch, count(brfr.fraud_type) as location_nro_fraud
        from
        analytics.source_fraud_m18 trans
        left join
        (select fr.*, br.id_location
            from receiver_fraud_fecha fr join viamericas.branch br
            on fr.id_branch = br.id_branch) brfr
        on trans.id_receiver = brfr.id_receiver and brfr.date_receiver < trans.DATE_RECEIVER
        and brfr.fraud_classification = 'Real'
        group by trans.id_location, trans.id_receiver, trans.id_branch
    ),
    sender_trans_3m_cte as (
        select sdtrans.id_sender_global, sdtrans.id_branch, sdtrans.id_receiver, count(rc.date_receiver) as sender_trans_3m, avg(rc.net_amount_receiver) as range_hist
        from
        (select trans.*, sd.id_sender
            from analytics.source_fraud_m18 trans join
            viamericas.sender sd on trans.id_sender_global = try(cast(sd.id_sender_global as integer))) sdtrans left join 
        viamericas.receiver rc
        on trim(rc.id_branch) = sdtrans.id_branch and try(cast(rc.id_sender as integer)) = try(cast(sdtrans.id_sender as integer))
        and rc.date_receiver between date_add('month',-3, sdtrans.DATE_RECEIVER) and  sdtrans.DATE_RECEIVER
        group by sdtrans.id_sender_global, sdtrans.id_branch, sdtrans.id_receiver
    ),
    sender_nro_fraud_cte as (
        select trans.id_sender_global, trans.id_branch, trans.id_receiver, count(sdfr.fraud_type) as sender_nro_fraud
        from
        analytics.source_fraud_m18 trans
        left join
        (select fr.*, sd.id_sender_global
            from receiver_fraud_fecha fr join viamericas.sender sd
            on fr.id_branch = sd.id_branch and fr.id_sender=sd.id_sender) sdfr
        on trans.id_receiver = sdfr.id_receiver and sdfr.date_receiver < trans.DATE_RECEIVER
        and sdfr.fraud_classification = 'Real'
        group by trans.id_sender_global, trans.id_branch, trans.id_receiver
    ),
    sender_state_cte as (
        select distinct sd.id_sender_global, sd.id_state as sender_state, sd.day
        from analytics.source_fraud_m18 trans join
            viamericas.sender sd on trans.id_sender_global = sd.id_sender_global
    )
    select
        sf.*,
        rff.fraud_type,
        rff.fraud_classification as fraud_classification_2,
        rff.incident_code,
        rff.id_country_receiver,
        rff.id_payment,
        rff.id_state_receiver,
        rff.id_city_receiver,
        rff.bank_receiver,
        bt3md.branch_trans_3m,
        bt3md.count_date_receiver_distinct,
        st.sender_state,
        st.day,
        bff.branch_has_fraud,
        bff.fecha_last_fraud_branch,
        rhf.receiver_has_fraud,
        bt4m.branch_trans_40min,
        bt10m.branch_trans_10min,
        cpu4m.cash_pick_up_4min,
        lnf.location_nro_fraud,
        st3m.sender_trans_3m,
        st3m.range_hist,
        snf.sender_nro_fraud
    from
        analytics.source_fraud_m18 sf
    left join analytics.branch_trans_3m_distinct_cte bt3md on sf.id_branch = bt3md.id_branch and sf.id_receiver = bt3md.id_receiver
    left join sender_trans_3m_cte st3m on sf.id_sender_global = st3m.id_sender_global and sf.id_branch = st3m.id_branch and sf.id_receiver = st3m.id_receiver
    left join sender_nro_fraud_cte snf on sf.id_sender_global = snf.id_sender_global and sf.id_branch = snf.id_branch and sf.id_receiver = snf.id_receiver
    left join sender_state_cte st on sf.id_sender_global = st.id_sender_global
    left join receiver_has_fraud_cte rhf on sf.id_receiver = try(cast(rhf.id_receiver as integer)) and sf.id_branch = rhf.id_branch
    left join branch_and_fecha_last_fraud bff on sf.id_branch = bff.id_branch and sf.id_receiver = bff.id_receiver
    left join analytics.branch_trans_40min_cte_lg bt4m on sf.id_branch = bt4m.id_branch and sf.id_receiver = bt4m.id_receiver
    left join analytics.branch_trans_10min_cte_lg bt10m on sf.id_branch = bt10m.id_branch and sf.id_receiver = bt10m.id_receiver
    left join analytics.cash_pick_up_4min_cte cpu4m on sf.id_branch = cpu4m.id_branch and sf.id_receiver = cpu4m.id_receiver
    left join receiver_fraud_fecha rff on sf.id_branch = rff.id_branch and sf.id_receiver = rff.id_receiver
    left join location_nro_fraud_cte lnf on sf.id_location = lnf.id_location and sf.id_branch = lnf.id_branch and sf.id_receiver = lnf.id_receiver
);






--- DROP TABLES AUXS

drop table analytics.source_fraud_m18;
drop table analytics.branch_trans_3m_cte_lg;
drop table analytics.branch_trans_4min_cte_lg;
drop table analytics.branch_trans_10min_cte_lg;
drop table analytics.branch_prom_diario_cte;
drop table analytics.cash_pick_up_4min_cte;


-- DROP ABT
drop table analytics.abt_fraud;
drop table analytics.abt_fraudv2;


-- El id sender global del state es el que duplica