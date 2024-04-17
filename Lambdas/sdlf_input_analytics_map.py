def lambda_handler(event, context):
    crawlers = {
        "CrawlerNames": [
            {"CrawlerName": "sdlf_crw_abt_source_generator_last_daily_forex_country"},
            {"CrawlerName": "sdlf_crw_abt_source_generator_daily_check_gp"},
            {
                "CrawlerName": "sdlf_crw_abt_source_generator_daily_sales_count_cancelled_v2"
            },
        ]
    }
    return crawlers
