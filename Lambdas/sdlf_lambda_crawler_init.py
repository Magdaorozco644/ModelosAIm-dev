# lambda_crawler

import json
import boto3
import time
from time import sleep


def wait_for_crawler_completion(crawler_name: str, poll_interval: int = 5) -> str:
    """
    Waits until Glue crawler completes and
    returns the status of the latest crawl run.
    Raises AirflowException if the crawler fails or is cancelled.
    :param crawler_name: unique crawler name per AWS account
    :param poll_interval: Time (in seconds) to wait between two consecutive calls to check crawler status
    :return: Crawler's status
    """
    client = boto3.client("glue")
    response = client.start_crawler(Name=crawler_name)
    failed_status = ["FAILED", "CANCELLED"]
    sleep(5)

    while True:
        crawler = client.get_crawler(Name=crawler_name)
        crawler_state = crawler["Crawler"]["State"]
        if "LastCrawl" in crawler["Crawler"]:
            if crawler_state == "STOPPING" or crawler_state == "READY":
                print("State: %s", crawler_state)
                print("crawler_config: %s", crawler)
                crawler_status = crawler["Crawler"]["LastCrawl"]["Status"]
                if crawler_status in failed_status:
                    raise Exception(f"Status: {crawler_status}")
                metrics = client.get_crawler_metrics(CrawlerNameList=[crawler_name])[
                    "CrawlerMetricsList"
                ][0]
                print("Status: %s", crawler_status)
                print(
                    "Last Runtime Duration (seconds): %s", metrics["LastRuntimeSeconds"]
                )
                print(
                    "Median Runtime Duration (seconds): %s",
                    metrics["MedianRuntimeSeconds"],
                )
                print("Tables Created: %s", metrics["TablesCreated"])
                print("Tables Updated: %s", metrics["TablesUpdated"])
                print("Tables Deleted: %s", metrics["TablesDeleted"])

                return crawler_status

            else:
                print("Polling for AWS Glue crawler: %s ", crawler_name)
                print("State: %s", crawler_state)

                metrics = client.get_crawler_metrics(CrawlerNameList=[crawler_name])[
                    "CrawlerMetricsList"
                ][0]
                time_left = int(metrics["TimeLeftSeconds"])

                if time_left > 0:
                    print("Estimated Time Left (seconds): %s", time_left)
                else:
                    print("Crawler should finish soon")

                time.sleep(poll_interval)
        else:
            if crawler_state == "READY":
                print("State: %s", crawler_state)
                print("crawler_config: %s", crawler)
                crawler_status = crawler["Crawler"]["LastCrawl"]["Status"]
                if crawler_status in failed_status:
                    raise Exception(f"Status: {crawler_status}")
                metrics = client.get_crawler_metrics(CrawlerNameList=[crawler_name])[
                    "CrawlerMetricsList"
                ][0]
                print("Status: %s", crawler_status)
                print(
                    "Last Runtime Duration (seconds): %s", metrics["LastRuntimeSeconds"]
                )
                print(
                    "Median Runtime Duration (seconds): %s",
                    metrics["MedianRuntimeSeconds"],
                )
                print("Tables Created: %s", metrics["TablesCreated"])
                print("Tables Updated: %s", metrics["TablesUpdated"])
                print("Tables Deleted: %s", metrics["TablesDeleted"])

                return crawler_status

            else:
                print("Polling for AWS Glue crawler: %s ", crawler_name)
                print("State: %s", crawler_state)

                metrics = client.get_crawler_metrics(CrawlerNameList=[crawler_name])[
                    "CrawlerMetricsList"
                ][0]
                time_left = int(metrics["TimeLeftSeconds"])

                if time_left > 0:
                    print("Estimated Time Left (seconds): %s", time_left)
                else:
                    print("Crawler should finish soon")

                time.sleep(poll_interval)


def lambda_handler(event, context):
    # TODO implement
    CrawlerName = event["CrawlerName"]
    crawler_to_run = f"{CrawlerName}"

    wait_for_crawler_completion(crawler_name=crawler_to_run)
