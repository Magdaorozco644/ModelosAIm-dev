create table analytics.daily_forex_symbol as (
select distinct feed_tbl.symbol, feed_tbl.max_feed_date, feed_tbl.max_feed_price, ctry.name_country as country, feed_tbl.day as day, ctry.id_country, trim(substring(feed_tbl.symbol,-3,3)) as last3_symbol, c.iso_4217
from (
    select
        symbol,
        MAX(feed_date) AS max_feed_date, MAX(feed_price) AS max_feed_price,
        day as day
    FROM "viamericas".forex_feed_market
    WHERE day > '2021-01-01'
    group by symbol, day) feed_tbl
left join viamericas.currency c
on trim(substring(feed_tbl.symbol,-3,3)) = trim(c.iso_code)
left join viamericas.country ctry
on trim(c.iso_4217) = trim(ctry.iso_n)
);

select * from analytics.daily_forex_symbol;


--------- VERSION CON COUNTRY CURRENCY

create table analytics.last_daily_forex_country_v2 as (
    select distinct feed_tbl.symbol, feed_tbl.max_feed_date, feed_tbl.max_feed_price, ctry.name_country as country, feed_tbl.day as day, c.id_country,
    trim(substring(feed_tbl.symbol,-3,3)) as symbol_feed_tbl, trim(c.id_country_currency) as id_country_currency
    from (
    	select
    	symbol,
    	MAX(feed_date) AS max_feed_date, MAX(feed_price) AS max_feed_price,
    	day as day
    	FROM viamericas.forex_feed_market
    	WHERE day > '2021-01-01'
    	group by symbol, day) feed_tbl
    left join viamericas.country_currency c
    on trim(substring(feed_tbl.symbol,-3,3)) = trim(c.id_country_currency)
    left join viamericas.country ctry
    on trim(ctry.id_country) = trim(c.id_country)
)
select * from analytics.last_daily_forex_country_v2;