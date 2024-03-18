import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import awswrangler as wr
import os
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
spark.conf.set("spark.sql.hive.metastorePartitionPruningFallbackOnException","true")


def create_last_daily_forex():
	#Check if target table exists
	input_db='viamericas'
	input_table='forex_feed_market'
	target_db='analytics'
	target_table='last_daily_forex_country'
	target_table_exist = wr.catalog.does_table_exist(database=target_db, table=target_table)
	input_table_exist = wr.catalog.does_table_exist(database=input_db, table=input_table)
	s3outputpath = "s3://viamericas-datalake-dev-us-east-1-283731589572-analytics/last_daily_forex_country/"


	if input_table_exist == True:
		#If table exists, just read the last partition, otherwise read the entire table and create the target
		if target_table_exist == True:
			print(f"Target table '{target_table}' exist, non calculated partitions will be created")
			current_target_partitions = list(wr.catalog.get_partitions(database=target_db, table=target_table).values())
			list_current_target_partitions = [f[0] for f in current_target_partitions]
			list_current_target_partitions.sort(reverse=True)
			current_input_partitions = list(wr.catalog.get_partitions(database=input_db, table=input_table).values())
			list_current_input_partitions = [f[0] for f in current_input_partitions]
			list_current_input_partitions.sort(reverse=True)
			#list_current_input_partitions = spark.sql(f""" select distinct day from {input_db}.{input_table} """).rdd.map(lambda x: x[0]).collect()
			#list_current_input_partitions.sort(reverse=True)
			days_to_process = list(set(list_current_input_partitions) - set(list_current_target_partitions))
			days_to_process.sort()
			paquete = " OR ".join([str('day') + " = " +"'"+str(b)+"'" for b in days_to_process])
			print(" Current target partitions: ", str(list_current_target_partitions))
			print(" Current input partitions: ", str(list_current_input_partitions))
			print(" Days to process: ", str(days_to_process))
			print(" Package expression: ", str(paquete))

			if len(days_to_process) == 0:
				df = spark.sql(f"""
                select distinct feed_tbl.symbol, feed_tbl.max_feed_date, feed_tbl.max_feed_price, c.name_country as country, feed_tbl.day as day
				from (
					select
					symbol,
					MAX(feed_date) AS max_feed_date, MAX(feed_price) AS max_feed_price,
					day as day
					FROM {input_db}.{input_table}
					WHERE day = '{list_current_input_partitions[0]}'
					group by symbol, day) feed_tbl
				left join {input_db}.trader_group tg
				on feed_tbl.symbol = tg.symbol
				left join {input_db}.country c
				on trim(tg.id_country) = trim(c.id_country)
				""")

				df = df.repartition("day")
				print("Total rows >>>>>>>>>>>> ", df.count())
	

				df \
					.write.mode('overwrite') \
					.format('parquet') \
					.partitionBy('day') \
					.save( s3outputpath )
			if len(days_to_process) > 0 and len(days_to_process) < 20:
				df = spark.sql(f"""
                select distinct feed_tbl.symbol, feed_tbl.max_feed_date, feed_tbl.max_feed_price, c.name_country as country, feed_tbl.day as day
				from (
					select
					symbol,
					MAX(feed_date) AS max_feed_date, MAX(feed_price) AS max_feed_price,
					day as day
					FROM {input_db}.{input_table}
					WHERE ({paquete})
					group by symbol, day) feed_tbl
				left join {input_db}.trader_group tg
				on feed_tbl.symbol = tg.symbol
				left join {input_db}.country c
				on trim(tg.id_country) = trim(c.id_country)
    			""")

				df = df.repartition("day")
				print("Total rows >>>>>>>>>>>> ", df.count())
	

				df \
					.write.mode('overwrite') \
					.format('parquet') \
					.partitionBy('day') \
					.save( s3outputpath )
			else:
				print('More than 20 partitions missing.')
				df = spark.sql(f"""
				select distinct feed_tbl.symbol, feed_tbl.max_feed_date, feed_tbl.max_feed_price, c.name_country as country, feed_tbl.day as day
				from (
					select
					symbol,
					MAX(feed_date) AS max_feed_date, MAX(feed_price) AS max_feed_price,
					day as day
					FROM {input_db}.{input_table}
					WHERE day >= '{days_to_process[0]}'
					group by symbol, day) feed_tbl
				left join {input_db}.trader_group tg
				on feed_tbl.symbol = tg.symbol
				left join {input_db}.country c
				on trim(tg.id_country) = trim(c.id_country)
				""")

				df = df.repartition("day")
				print("Total rows >>>>>>>>>>>> ", df.count())


				df \
					.write.mode('overwrite') \
					.format('parquet') \
					.partitionBy('day') \
					.save( s3outputpath )

		else:
			print("Target table does not exist, will be created")
			df = spark.sql(f"""
            select distinct feed_tbl.symbol, feed_tbl.max_feed_date, feed_tbl.max_feed_price, c.name_country as country, feed_tbl.day as day
			from (
				select
				symbol,
				MAX(feed_date) AS max_feed_date, MAX(feed_price) AS max_feed_price,
				day as day
				FROM {input_db}.{input_table}
				WHERE day > '2021-01-01'
				group by symbol, day) feed_tbl
			left join {input_db}.trader_group tg
			on feed_tbl.symbol = tg.symbol
			left join {input_db}.country c
			on trim(tg.id_country) = trim(c.id_country)
   			""")

			df = df.repartition("day")
			print("Total rows >>>>>>>>>>>> ", df.count())


			df \
				.write.mode('overwrite') \
				.format('parquet') \
				.partitionBy('day') \
				.save( s3outputpath )

	else:
		print("Input table does not exist, job will exit with the following exception")
		raise Exception("FATAL: Input table, ", input_db, ".", input_table, " does not exist!!! ")
		os._exit()

def create_daily_check():
	#Check if target table exists
	input_db='viamericas'
	input_table='receiver'
	target_db='analytics'
	target_table='daily_check'
	target_table_exist = wr.catalog.does_table_exist(database=target_db, table=target_table)
	input_table_exist = wr.catalog.does_table_exist(database=input_db, table=input_table)
	s3outputpath = "s3://viamericas-datalake-dev-us-east-1-283731589572-analytics/daily_check/"


	if input_table_exist == True:
		#If table exists, just read the last partition, otherwise read the entire table and create the target
		if target_table_exist == True:
			print(f"Target table '{target_table}' exist, non calculated partitions will be created")
			current_target_partitions = list(wr.catalog.get_partitions(database=target_db, table=target_table).values())
			list_current_target_partitions = [f[0] for f in current_target_partitions]
			list_current_target_partitions = list(filter(lambda x: int(x.replace("-","")) > 20201231, list_current_target_partitions))
			list_current_target_partitions.sort(reverse=True)
			current_input_partitions = list(wr.catalog.get_partitions(database=input_db, table=input_table).values())
			list_current_input_partitions = [f[0] for f in current_input_partitions]
			list_current_input_partitions = list(filter(lambda x: int(x.replace("-","")) > 20201231, list_current_input_partitions))
			list_current_input_partitions.sort(reverse=True)
			#list_current_input_partitions = spark.sql(f""" select distinct day from {input_db}.{input_table} """).rdd.map(lambda x: x[0]).collect()
			#list_current_input_partitions.sort(reverse=True)
			days_to_process = list(set(list_current_input_partitions) - set(list_current_target_partitions))
			days_to_process.sort()
			paquete = " OR ".join([str('day') + " = " +"'"+str(b)+"'" for b in days_to_process])
			print(" Current target partitions: ", str(list_current_target_partitions))
			print(" Current input partitions: ", str(list_current_input_partitions))
			print(" Days to process: ", str(days_to_process))
			print(" Package expression: ", str(paquete))

			if len(days_to_process) == 0:
				df = spark.sql(f"""
				SELECT
				CAST(RTRIM(p.NAME_MAIN_BRANCH) AS VARCHAR(60)) AS PAYER,
				RTRIM(co.NAME_COUNTRY) AS COUNTRY,
				day as DATE,
				SUM(CASE WHEN A.ID_FLAG_RECEIVER = 'A' OR A.ID_FLAG_RECEIVER = 'C' THEN 0 ELSE 1 END) AS TX,
				SUM(CASE WHEN A.ID_FLAG_RECEIVER = 'A' OR A.ID_FLAG_RECEIVER = 'C' THEN 0 ELSE A.NET_AMOUNT_RECEIVER END) AS AMOUNT,
				COUNT(a.COUPON_CODE) AS COUPON_COUNT,
				day
				FROM
				viamericas.RECEIVER a
				INNER JOIN viamericas.GROUP_BRANCH p ON p.ID_MAIN_BRANCH = CASE WHEN a.ID_MAIN_BRANCH_EXPIRED IS NULL THEN RTRIM(a.ID_MAIN_BRANCH_SENT) ELSE RTRIM(a.ID_MAIN_BRANCH_EXPIRED) END
				INNER JOIN viamericas.COUNTRY co ON a.ID_COUNTRY_RECEIVER = co.ID_COUNTRY
				LEFT JOIN viamericas.BRANCH b ON a.ID_BRANCH = b.ID_BRANCH
				WHERE day = '{list_current_input_partitions[0]}' AND
				NOT (A.ID_MAIN_BRANCH_SENT LIKE 'M%')
				AND NOT (A.ID_BRANCH LIKE 'T%')
				AND SUBSTRING(b.id_branch, 1, 1) IN (SELECT SUBSTRING(PREFIX, 1, 1) FROM viamericas.BRANCH_PREFIX P WHERE TRIM(B.ID_COUNTRY) = TRIM(P.ID_COUNTRY))
				AND b.ID_LOCATION IS NOT NULL
				AND b.ID_LOCATION NOT IN ('MD0010', 'MD0952', 'AK0003', 'CA3897', 'NY1130', 'MD0696', 'FL1933', 'AK0004', 'CA4046', 'NY1221', 'MD0623', 'MD1003', 'MD1018', 'AK0008', 'CA4291', 'NY1346', 'CA4350', 'NY1381', 'OK0236', 'FL2287', 'CA4391', 'AK0009', 'NY1397', 'CA4392', 'AK0010', 'FL2288', 'NY1399', 'AK0012', 'CA4396', 'NY1402', 'FL2289', 'AK0013', 'CA4418', 'NY1410', 'FL2301', 'AK0014', 'CA4428', 'NY1413')
				AND a.NET_AMOUNT_RECEIVER <> 0
				GROUP BY
				RTRIM(p.NAME_MAIN_BRANCH),
				RTRIM(co.NAME_COUNTRY),
				day,
				day """)

				df = df.repartition("day")
	

				df \
					.write.mode('overwrite') \
					.format('parquet') \
					.partitionBy('day') \
					.save( s3outputpath )
			if len(days_to_process) > 0 and len(days_to_process) < 20:
				df = spark.sql(f"""
				SELECT
				CAST(RTRIM(p.NAME_MAIN_BRANCH) AS VARCHAR(60)) AS PAYER,
				RTRIM(co.NAME_COUNTRY) AS COUNTRY,
				day as DATE,
				SUM(CASE WHEN A.ID_FLAG_RECEIVER = 'A' OR A.ID_FLAG_RECEIVER = 'C' THEN 0 ELSE 1 END) AS TX,
				SUM(CASE WHEN A.ID_FLAG_RECEIVER = 'A' OR A.ID_FLAG_RECEIVER = 'C' THEN 0 ELSE A.NET_AMOUNT_RECEIVER END) AS AMOUNT,
				COUNT(a.COUPON_CODE) AS COUPON_COUNT,
				day
				FROM
				viamericas.RECEIVER a
				INNER JOIN viamericas.GROUP_BRANCH p ON p.ID_MAIN_BRANCH = CASE WHEN a.ID_MAIN_BRANCH_EXPIRED IS NULL THEN RTRIM(a.ID_MAIN_BRANCH_SENT) ELSE RTRIM(a.ID_MAIN_BRANCH_EXPIRED) END
				INNER JOIN viamericas.COUNTRY co ON a.ID_COUNTRY_RECEIVER = co.ID_COUNTRY
				LEFT JOIN viamericas.BRANCH b ON a.ID_BRANCH = b.ID_BRANCH
				WHERE ({paquete}) AND
				NOT (A.ID_MAIN_BRANCH_SENT LIKE 'M%')
				AND NOT (A.ID_BRANCH LIKE 'T%')
				AND SUBSTRING(b.id_branch, 1, 1) IN (SELECT SUBSTRING(PREFIX, 1, 1) FROM viamericas.BRANCH_PREFIX P WHERE TRIM(B.ID_COUNTRY) = TRIM(P.ID_COUNTRY))
				AND b.ID_LOCATION IS NOT NULL
				AND b.ID_LOCATION NOT IN ('MD0010', 'MD0952', 'AK0003', 'CA3897', 'NY1130', 'MD0696', 'FL1933', 'AK0004', 'CA4046', 'NY1221', 'MD0623', 'MD1003', 'MD1018', 'AK0008', 'CA4291', 'NY1346', 'CA4350', 'NY1381', 'OK0236', 'FL2287', 'CA4391', 'AK0009', 'NY1397', 'CA4392', 'AK0010', 'FL2288', 'NY1399', 'AK0012', 'CA4396', 'NY1402', 'FL2289', 'AK0013', 'CA4418', 'NY1410', 'FL2301', 'AK0014', 'CA4428', 'NY1413')
				AND a.NET_AMOUNT_RECEIVER <> 0
				GROUP BY
				RTRIM(p.NAME_MAIN_BRANCH),
				RTRIM(co.NAME_COUNTRY),
				day,
				day """)

				df = df.repartition("day")

				df \
					.write.mode('overwrite') \
					.format('parquet') \
					.partitionBy('day') \
					.save( s3outputpath )

		else:
			print("Target table does not exist, will be created")
			df = spark.sql(f"""
				SELECT
				CAST(RTRIM(p.NAME_MAIN_BRANCH) AS VARCHAR(60)) AS PAYER,
				RTRIM(co.NAME_COUNTRY) AS COUNTRY,
				day as DATE,
				SUM(CASE WHEN A.ID_FLAG_RECEIVER = 'A' OR A.ID_FLAG_RECEIVER = 'C' THEN 0 ELSE 1 END) AS TX,
				SUM(CASE WHEN A.ID_FLAG_RECEIVER = 'A' OR A.ID_FLAG_RECEIVER = 'C' THEN 0 ELSE A.NET_AMOUNT_RECEIVER END) AS AMOUNT,
				COUNT(a.COUPON_CODE) AS COUPON_COUNT,
				day
				FROM
				viamericas.RECEIVER a
				INNER JOIN viamericas.GROUP_BRANCH p ON p.ID_MAIN_BRANCH = CASE WHEN a.ID_MAIN_BRANCH_EXPIRED IS NULL THEN RTRIM(a.ID_MAIN_BRANCH_SENT) ELSE RTRIM(a.ID_MAIN_BRANCH_EXPIRED) END
				INNER JOIN viamericas.COUNTRY co ON a.ID_COUNTRY_RECEIVER = co.ID_COUNTRY
				LEFT JOIN viamericas.BRANCH b ON a.ID_BRANCH = b.ID_BRANCH
				WHERE
				NOT (A.ID_MAIN_BRANCH_SENT LIKE 'M%')
				AND NOT (A.ID_BRANCH LIKE 'T%')
				AND SUBSTRING(b.id_branch, 1, 1) IN (SELECT SUBSTRING(PREFIX, 1, 1) FROM viamericas.BRANCH_PREFIX P WHERE TRIM(B.ID_COUNTRY) = TRIM(P.ID_COUNTRY))
				AND day >= '2021-01-01'
				AND b.ID_LOCATION IS NOT NULL
				AND b.ID_LOCATION NOT IN ('MD0010', 'MD0952', 'AK0003', 'CA3897', 'NY1130', 'MD0696', 'FL1933', 'AK0004', 'CA4046', 'NY1221', 'MD0623', 'MD1003', 'MD1018', 'AK0008', 'CA4291', 'NY1346', 'CA4350', 'NY1381', 'OK0236', 'FL2287', 'CA4391', 'AK0009', 'NY1397', 'CA4392', 'AK0010', 'FL2288', 'NY1399', 'AK0012', 'CA4396', 'NY1402', 'FL2289', 'AK0013', 'CA4418', 'NY1410', 'FL2301', 'AK0014', 'CA4428', 'NY1413')
				AND a.NET_AMOUNT_RECEIVER <> 0
				GROUP BY
				RTRIM(p.NAME_MAIN_BRANCH),
				RTRIM(co.NAME_COUNTRY),
				day,
				day """)

			df = df.repartition("day")


			df \
				.write.mode('overwrite') \
				.format('parquet') \
				.partitionBy('day') \
				.save( s3outputpath )

	else:
		print("Input table does not exist, job will exit with the following exception")
		raise Exception("FATAL: Input table, ", input_db, ".", input_table, " does not exist!!! ")
		os._exit()

def create_daily_check_gp():
	#Check if target table exists
	input_db='viamericas'
	input_table='receiver'
	target_db='analytics'
	target_table='daily_check_gp'
	target_table_exist = wr.catalog.does_table_exist(database=target_db, table=target_table)
	input_table_exist = wr.catalog.does_table_exist(database=input_db, table=input_table)
	s3outputpath = "s3://viamericas-datalake-dev-us-east-1-283731589572-analytics/daily_check_gp/"


	if input_table_exist == True:
		#If table exists, just read the last partition, otherwise read the entire table and create the target
		if target_table_exist == True:
			print(f"Target table '{target_table}' exist, non calculated partitions will be created")
			current_target_partitions = list(wr.catalog.get_partitions(database=target_db, table=target_table).values())
			list_current_target_partitions = [f[0] for f in current_target_partitions]
			list_current_target_partitions = list(filter(lambda x: int(x.replace("-","")) > 20201231, list_current_target_partitions))
			list_current_target_partitions.sort(reverse=True)
			current_input_partitions = list(wr.catalog.get_partitions(database=input_db, table=input_table).values())
			list_current_input_partitions = [f[0] for f in current_input_partitions]
			list_current_input_partitions = list(filter(lambda x: int(x.replace("-","")) > 20201231, list_current_input_partitions))
			list_current_input_partitions.sort(reverse=True)
			#list_current_input_partitions = spark.sql(f""" select distinct day from {input_db}.{input_table} """).rdd.map(lambda x: x[0]).collect()
			#list_current_input_partitions.sort(reverse=True)
			days_to_process = list(set(list_current_input_partitions) - set(list_current_target_partitions))
			days_to_process.sort()
			paquete = " OR ".join([str('a.day') + " = " +"'"+str(b)+"'" for b in days_to_process])
			print(" Current target partitions: ", str(list_current_target_partitions))
			print(" Current input partitions: ", str(list_current_input_partitions))
			print(" Days to process: ", str(days_to_process))
			print(" Package expression: ", str(paquete))

			if len(days_to_process) == 0:
				df = spark.sql(f"""
                    SELECT
					CAST(RTRIM(p.NAME_MAIN_BRANCH) AS VARCHAR(60)) AS PAYER,
					RTRIM(co.NAME_COUNTRY) AS COUNTRY,
					a.day as DATE,
					SUM(CASE WHEN A.ID_FLAG_RECEIVER = 'A' OR A.ID_FLAG_RECEIVER = 'C' THEN 0 ELSE 1 END) AS TX,
					SUM(CASE WHEN A.ID_FLAG_RECEIVER = 'A' OR A.ID_FLAG_RECEIVER = 'C' THEN 0 ELSE A.NET_AMOUNT_RECEIVER END) AS AMOUNT,
					COUNT(a.COUPON_CODE) AS COUPON_COUNT,
					(
						---CASE WHEN A.ID_FLAG_RECEIVER = 'A' THEN 0 ELSE
							SUM(A.TELEX_RECEIVER + A.EXCHANGE_RECEIVER + A.TOTAL_MODO_PAGO + A.HANDLING_RECEIVER) - --[FEE],
							SUM((A.TELEX_RECEIVER + A.EXCHANGE_RECEIVER + A.TOTAL_MODO_PAGO) - (A.TELEX_COMPANY + A.EXCHANGE_COMPANY + A.TOTAL_MODO_PAGO_COMP + A.HANDLING_RECEIVER)) - --[FEE_AGENT]
							SUM(COALESCE(a.COMMISSION_PAYEE, 0)) + --FEE PAYER
							SUM(COALESCE(A.DISCOUNT, 0)) + --LOYALTY
							SUM(CASE
							WHEN YEAR(a.DATE_RECEIVER) < 2018 THEN COALESCE(a.FOREX_GAIN, COALESCE(a.FOREX_ESTIMATED, 0))
							ELSE CASE WHEN a.FOREX_CALC = 'Y' THEN a.FOREX_GAIN ELSE COALESCE(a.FOREX_ESTIMATED, 0) END
							END) - --FOREX
							SUM(CASE
								WHEN ORIGINAL_RATE <> 0 AND CAST((NET_AMOUNT_RECEIVER * ((ORIGINAL_RATE-RATE_CHANGE_RECEIVER)/ORIGINAL_RATE)) as DECIMAL(10,4)) - FEE_RATE*-1 < 0
									THEN
										CASE COALESCE(ORIGINAL_RATE,-999.999)
											WHEN -999.999 THEN FEE_RATE*-1
											WHEN 0 THEN 0
											ELSE CAST((NET_AMOUNT_RECEIVER * ((ORIGINAL_RATE-RATE_CHANGE_RECEIVER)/ORIGINAL_RATE)) as decimal(10,4))
										END
									ELSE CAST(FEE_RATE*-1 AS decimal(12, 2))
							END) - --VIATASA
							SUM(CAST((CASE WHEN A.ID_FLAG_RECEIVER IN('A','C') THEN 0 ELSE COALESCE(vd.VIADEAL_REAL, COALESCE(vd.VIADEAL_ESTIMATED,0)) END) as decimal(18, 8)) ) -- VIADEAL
						--END
					) as GP,
				a.day as day
				FROM
					viamericas.RECEIVER a
				INNER JOIN viamericas.GROUP_BRANCH p ON p.ID_MAIN_BRANCH = CASE WHEN a.ID_MAIN_BRANCH_EXPIRED IS NULL THEN RTRIM(a.ID_MAIN_BRANCH_SENT) ELSE RTRIM(a.ID_MAIN_BRANCH_EXPIRED) END
				INNER JOIN viamericas.COUNTRY co ON a.ID_COUNTRY_RECEIVER = co.ID_COUNTRY
				LEFT JOIN viamericas.BRANCH b ON a.ID_BRANCH = b.ID_BRANCH
				LEFT JOIN viamericas.RECEIVER_GP_COMPONENTS vd ON a.ID_BRANCH = vd.ID_BRANCH and a.ID_RECEIVER = vd.ID_RECEIVER
				WHERE
    				a.day = '{list_current_input_partitions[0]}' AND
					NOT (A.ID_MAIN_BRANCH_SENT LIKE 'M%')
					AND NOT (A.ID_BRANCH LIKE 'T%')
					AND SUBSTRING(b.id_branch, 1, 1) IN (SELECT SUBSTRING(PREFIX, 1, 1) FROM viamericas.BRANCH_PREFIX P WHERE TRIM(B.ID_COUNTRY) = TRIM(P.ID_COUNTRY))
					AND b.ID_LOCATION IS NOT NULL
					AND b.ID_LOCATION NOT IN ('MD0010', 'MD0952', 'AK0003', 'CA3897', 'NY1130', 'MD0696', 'FL1933', 'AK0004', 'CA4046', 'NY1221', 'MD0623', 'MD1003', 'MD1018', 'AK0008', 'CA4291', 'NY1346', 'CA4350', 'NY1381', 'OK0236', 'FL2287', 'CA4391', 'AK0009', 'NY1397', 'CA4392', 'AK0010', 'FL2288', 'NY1399', 'AK0012', 'CA4396', 'NY1402', 'FL2289', 'AK0013', 'CA4418', 'NY1410', 'FL2301', 'AK0014', 'CA4428', 'NY1413')
					AND a.NET_AMOUNT_RECEIVER <> 0
				GROUP BY
					RTRIM(p.NAME_MAIN_BRANCH),
					RTRIM(co.NAME_COUNTRY),
					a.day
				""")

				df = df.repartition("day")


				df \
					.write.mode('overwrite') \
					.format('parquet') \
					.partitionBy('day') \
					.save( s3outputpath )
			if len(days_to_process) > 0 and len(days_to_process) < 20:
				df = spark.sql(f"""
                    SELECT
					CAST(RTRIM(p.NAME_MAIN_BRANCH) AS VARCHAR(60)) AS PAYER,
					RTRIM(co.NAME_COUNTRY) AS COUNTRY,
					a.day as DATE,
					SUM(CASE WHEN A.ID_FLAG_RECEIVER = 'A' OR A.ID_FLAG_RECEIVER = 'C' THEN 0 ELSE 1 END) AS TX,
					SUM(CASE WHEN A.ID_FLAG_RECEIVER = 'A' OR A.ID_FLAG_RECEIVER = 'C' THEN 0 ELSE A.NET_AMOUNT_RECEIVER END) AS AMOUNT,
					COUNT(a.COUPON_CODE) AS COUPON_COUNT,
					(
						---CASE WHEN A.ID_FLAG_RECEIVER = 'A' THEN 0 ELSE
							SUM(A.TELEX_RECEIVER + A.EXCHANGE_RECEIVER + A.TOTAL_MODO_PAGO + A.HANDLING_RECEIVER) - --[FEE],
							SUM((A.TELEX_RECEIVER + A.EXCHANGE_RECEIVER + A.TOTAL_MODO_PAGO) - (A.TELEX_COMPANY + A.EXCHANGE_COMPANY + A.TOTAL_MODO_PAGO_COMP + A.HANDLING_RECEIVER)) - --[FEE_AGENT]
							SUM(COALESCE(a.COMMISSION_PAYEE, 0)) + --FEE PAYER
							SUM(COALESCE(A.DISCOUNT, 0)) + --LOYALTY
							SUM(CASE
							WHEN YEAR(a.DATE_RECEIVER) < 2018 THEN COALESCE(a.FOREX_GAIN, COALESCE(a.FOREX_ESTIMATED, 0))
							ELSE CASE WHEN a.FOREX_CALC = 'Y' THEN a.FOREX_GAIN ELSE COALESCE(a.FOREX_ESTIMATED, 0) END
							END) - --FOREX
							SUM(CASE
								WHEN ORIGINAL_RATE <> 0 AND CAST((NET_AMOUNT_RECEIVER * ((ORIGINAL_RATE-RATE_CHANGE_RECEIVER)/ORIGINAL_RATE)) as DECIMAL(10,4)) - FEE_RATE*-1 < 0
									THEN
										CASE COALESCE(ORIGINAL_RATE,-999.999)
											WHEN -999.999 THEN FEE_RATE*-1
											WHEN 0 THEN 0
											ELSE CAST((NET_AMOUNT_RECEIVER * ((ORIGINAL_RATE-RATE_CHANGE_RECEIVER)/ORIGINAL_RATE)) as decimal(10,4))
										END
									ELSE CAST(FEE_RATE*-1 AS decimal(12, 2))
							END) - --VIATASA
							SUM(CAST((CASE WHEN A.ID_FLAG_RECEIVER IN('A','C') THEN 0 ELSE COALESCE(vd.VIADEAL_REAL, COALESCE(vd.VIADEAL_ESTIMATED,0)) END) as decimal(18, 8)) ) -- VIADEAL
						--END
					) as GP,
				a.day as day
				FROM
					viamericas.RECEIVER a
				INNER JOIN viamericas.GROUP_BRANCH p ON p.ID_MAIN_BRANCH = CASE WHEN a.ID_MAIN_BRANCH_EXPIRED IS NULL THEN RTRIM(a.ID_MAIN_BRANCH_SENT) ELSE RTRIM(a.ID_MAIN_BRANCH_EXPIRED) END
				INNER JOIN viamericas.COUNTRY co ON a.ID_COUNTRY_RECEIVER = co.ID_COUNTRY
				LEFT JOIN viamericas.BRANCH b ON a.ID_BRANCH = b.ID_BRANCH
				LEFT JOIN viamericas.RECEIVER_GP_COMPONENTS vd ON a.ID_BRANCH = vd.ID_BRANCH and a.ID_RECEIVER = vd.ID_RECEIVER
				WHERE
					({paquete}) AND
					NOT (A.ID_MAIN_BRANCH_SENT LIKE 'M%')
					AND NOT (A.ID_BRANCH LIKE 'T%')
					AND SUBSTRING(b.id_branch, 1, 1) IN (SELECT SUBSTRING(PREFIX, 1, 1) FROM viamericas.BRANCH_PREFIX P WHERE TRIM(B.ID_COUNTRY) = TRIM(P.ID_COUNTRY))
					AND b.ID_LOCATION IS NOT NULL
					AND b.ID_LOCATION NOT IN ('MD0010', 'MD0952', 'AK0003', 'CA3897', 'NY1130', 'MD0696', 'FL1933', 'AK0004', 'CA4046', 'NY1221', 'MD0623', 'MD1003', 'MD1018', 'AK0008', 'CA4291', 'NY1346', 'CA4350', 'NY1381', 'OK0236', 'FL2287', 'CA4391', 'AK0009', 'NY1397', 'CA4392', 'AK0010', 'FL2288', 'NY1399', 'AK0012', 'CA4396', 'NY1402', 'FL2289', 'AK0013', 'CA4418', 'NY1410', 'FL2301', 'AK0014', 'CA4428', 'NY1413')
					AND a.NET_AMOUNT_RECEIVER <> 0
				GROUP BY
					RTRIM(p.NAME_MAIN_BRANCH),
					RTRIM(co.NAME_COUNTRY),
					a.day
     			""")

				df = df.repartition("day")

				df \
					.write.mode('overwrite') \
					.format('parquet') \
					.partitionBy('day') \
					.save( s3outputpath )
			else:
				print('More than 20 partitions missings.')
				df = spark.sql(f"""
						SELECT
						CAST(RTRIM(p.NAME_MAIN_BRANCH) AS VARCHAR(60)) AS PAYER,
						RTRIM(co.NAME_COUNTRY) AS COUNTRY,
						a.day as DATE,
						SUM(CASE WHEN A.ID_FLAG_RECEIVER = 'A' OR A.ID_FLAG_RECEIVER = 'C' THEN 0 ELSE 1 END) AS TX,
						SUM(CASE WHEN A.ID_FLAG_RECEIVER = 'A' OR A.ID_FLAG_RECEIVER = 'C' THEN 0 ELSE A.NET_AMOUNT_RECEIVER END) AS AMOUNT,
						COUNT(a.COUPON_CODE) AS COUPON_COUNT,
						(
							---CASE WHEN A.ID_FLAG_RECEIVER = 'A' THEN 0 ELSE
								SUM(A.TELEX_RECEIVER + A.EXCHANGE_RECEIVER + A.TOTAL_MODO_PAGO + A.HANDLING_RECEIVER) - --[FEE],
								SUM((A.TELEX_RECEIVER + A.EXCHANGE_RECEIVER + A.TOTAL_MODO_PAGO) - (A.TELEX_COMPANY + A.EXCHANGE_COMPANY + A.TOTAL_MODO_PAGO_COMP + A.HANDLING_RECEIVER)) - --[FEE_AGENT]
								SUM(COALESCE(a.COMMISSION_PAYEE, 0)) + --FEE PAYER
								SUM(COALESCE(A.DISCOUNT, 0)) + --LOYALTY
								SUM(CASE
								WHEN YEAR(a.DATE_RECEIVER) < 2018 THEN COALESCE(a.FOREX_GAIN, COALESCE(a.FOREX_ESTIMATED, 0))
								ELSE CASE WHEN a.FOREX_CALC = 'Y' THEN a.FOREX_GAIN ELSE COALESCE(a.FOREX_ESTIMATED, 0) END
								END) - --FOREX
								SUM(CASE
									WHEN ORIGINAL_RATE <> 0 AND CAST((NET_AMOUNT_RECEIVER * ((ORIGINAL_RATE-RATE_CHANGE_RECEIVER)/ORIGINAL_RATE)) as DECIMAL(10,4)) - FEE_RATE*-1 < 0
										THEN
											CASE COALESCE(ORIGINAL_RATE,-999.999)
												WHEN -999.999 THEN FEE_RATE*-1
												WHEN 0 THEN 0
												ELSE CAST((NET_AMOUNT_RECEIVER * ((ORIGINAL_RATE-RATE_CHANGE_RECEIVER)/ORIGINAL_RATE)) as decimal(10,4))
											END
										ELSE CAST(FEE_RATE*-1 AS decimal(12, 2))
								END) - --VIATASA
								SUM(CAST((CASE WHEN A.ID_FLAG_RECEIVER IN('A','C') THEN 0 ELSE COALESCE(vd.VIADEAL_REAL, COALESCE(vd.VIADEAL_ESTIMATED,0)) END) as decimal(18, 8)) ) -- VIADEAL
							--END
						) as GP,
					a.day as day
					FROM
						viamericas.RECEIVER a
					INNER JOIN viamericas.GROUP_BRANCH p ON p.ID_MAIN_BRANCH = CASE WHEN a.ID_MAIN_BRANCH_EXPIRED IS NULL THEN RTRIM(a.ID_MAIN_BRANCH_SENT) ELSE RTRIM(a.ID_MAIN_BRANCH_EXPIRED) END
					INNER JOIN viamericas.COUNTRY co ON a.ID_COUNTRY_RECEIVER = co.ID_COUNTRY
					LEFT JOIN viamericas.BRANCH b ON a.ID_BRANCH = b.ID_BRANCH
					LEFT JOIN viamericas.RECEIVER_GP_COMPONENTS vd ON a.ID_BRANCH = vd.ID_BRANCH and a.ID_RECEIVER = vd.ID_RECEIVER
					WHERE
						NOT (A.ID_MAIN_BRANCH_SENT LIKE 'M%')
						AND NOT (A.ID_BRANCH LIKE 'T%')
						AND SUBSTRING(b.id_branch, 1, 1) IN (SELECT SUBSTRING(PREFIX, 1, 1) FROM viamericas.BRANCH_PREFIX P WHERE TRIM(B.ID_COUNTRY) = TRIM(P.ID_COUNTRY))
						AND a.day >= '{days_to_process[0]}'
						AND b.ID_LOCATION IS NOT NULL
						AND b.ID_LOCATION NOT IN ('MD0010', 'MD0952', 'AK0003', 'CA3897', 'NY1130', 'MD0696', 'FL1933', 'AK0004', 'CA4046', 'NY1221', 'MD0623', 'MD1003', 'MD1018', 'AK0008', 'CA4291', 'NY1346', 'CA4350', 'NY1381', 'OK0236', 'FL2287', 'CA4391', 'AK0009', 'NY1397', 'CA4392', 'AK0010', 'FL2288', 'NY1399', 'AK0012', 'CA4396', 'NY1402', 'FL2289', 'AK0013', 'CA4418', 'NY1410', 'FL2301', 'AK0014', 'CA4428', 'NY1413')
						AND a.NET_AMOUNT_RECEIVER <> 0
					GROUP BY
						RTRIM(p.NAME_MAIN_BRANCH),
						RTRIM(co.NAME_COUNTRY),
						a.day
					""")

				df = df.repartition("day")


				df \
					.write.mode('overwrite') \
					.format('parquet') \
					.partitionBy('day') \
					.save( s3outputpath )
		else:
			print("Target table does not exist, will be created")
			df = spark.sql(f"""
                  	SELECT
					CAST(RTRIM(p.NAME_MAIN_BRANCH) AS VARCHAR(60)) AS PAYER,
					RTRIM(co.NAME_COUNTRY) AS COUNTRY,
					a.day as DATE,
					SUM(CASE WHEN A.ID_FLAG_RECEIVER = 'A' OR A.ID_FLAG_RECEIVER = 'C' THEN 0 ELSE 1 END) AS TX,
					SUM(CASE WHEN A.ID_FLAG_RECEIVER = 'A' OR A.ID_FLAG_RECEIVER = 'C' THEN 0 ELSE A.NET_AMOUNT_RECEIVER END) AS AMOUNT,
					COUNT(a.COUPON_CODE) AS COUPON_COUNT,
					(
						---CASE WHEN A.ID_FLAG_RECEIVER = 'A' THEN 0 ELSE
							SUM(A.TELEX_RECEIVER + A.EXCHANGE_RECEIVER + A.TOTAL_MODO_PAGO + A.HANDLING_RECEIVER) - --[FEE],
							SUM((A.TELEX_RECEIVER + A.EXCHANGE_RECEIVER + A.TOTAL_MODO_PAGO) - (A.TELEX_COMPANY + A.EXCHANGE_COMPANY + A.TOTAL_MODO_PAGO_COMP + A.HANDLING_RECEIVER)) - --[FEE_AGENT]
							SUM(COALESCE(a.COMMISSION_PAYEE, 0)) + --FEE PAYER
							SUM(COALESCE(A.DISCOUNT, 0)) + --LOYALTY
							SUM(CASE
							WHEN YEAR(a.DATE_RECEIVER) < 2018 THEN COALESCE(a.FOREX_GAIN, COALESCE(a.FOREX_ESTIMATED, 0))
							ELSE CASE WHEN a.FOREX_CALC = 'Y' THEN a.FOREX_GAIN ELSE COALESCE(a.FOREX_ESTIMATED, 0) END
							END) - --FOREX
							SUM(CASE
								WHEN ORIGINAL_RATE <> 0 AND CAST((NET_AMOUNT_RECEIVER * ((ORIGINAL_RATE-RATE_CHANGE_RECEIVER)/ORIGINAL_RATE)) as DECIMAL(10,4)) - FEE_RATE*-1 < 0
									THEN
										CASE COALESCE(ORIGINAL_RATE,-999.999)
											WHEN -999.999 THEN FEE_RATE*-1
											WHEN 0 THEN 0
											ELSE CAST((NET_AMOUNT_RECEIVER * ((ORIGINAL_RATE-RATE_CHANGE_RECEIVER)/ORIGINAL_RATE)) as decimal(10,4))
										END
									ELSE CAST(FEE_RATE*-1 AS decimal(12, 2))
							END) - --VIATASA
							SUM(CAST((CASE WHEN A.ID_FLAG_RECEIVER IN('A','C') THEN 0 ELSE COALESCE(vd.VIADEAL_REAL, COALESCE(vd.VIADEAL_ESTIMATED,0)) END) as decimal(18, 8)) ) -- VIADEAL
						--END
					) as GP,
				a.day as day
				FROM
					viamericas.RECEIVER a
				INNER JOIN viamericas.GROUP_BRANCH p ON p.ID_MAIN_BRANCH = CASE WHEN a.ID_MAIN_BRANCH_EXPIRED IS NULL THEN RTRIM(a.ID_MAIN_BRANCH_SENT) ELSE RTRIM(a.ID_MAIN_BRANCH_EXPIRED) END
				INNER JOIN viamericas.COUNTRY co ON a.ID_COUNTRY_RECEIVER = co.ID_COUNTRY
				LEFT JOIN viamericas.BRANCH b ON a.ID_BRANCH = b.ID_BRANCH
				LEFT JOIN viamericas.RECEIVER_GP_COMPONENTS vd ON a.ID_BRANCH = vd.ID_BRANCH and a.ID_RECEIVER = vd.ID_RECEIVER
				WHERE
					NOT (A.ID_MAIN_BRANCH_SENT LIKE 'M%')
					AND NOT (A.ID_BRANCH LIKE 'T%')
					AND SUBSTRING(b.id_branch, 1, 1) IN (SELECT SUBSTRING(PREFIX, 1, 1) FROM viamericas.BRANCH_PREFIX P WHERE TRIM(B.ID_COUNTRY) = TRIM(P.ID_COUNTRY))
					AND a.day >= '2021-01-01'
					AND b.ID_LOCATION IS NOT NULL
					AND b.ID_LOCATION NOT IN ('MD0010', 'MD0952', 'AK0003', 'CA3897', 'NY1130', 'MD0696', 'FL1933', 'AK0004', 'CA4046', 'NY1221', 'MD0623', 'MD1003', 'MD1018', 'AK0008', 'CA4291', 'NY1346', 'CA4350', 'NY1381', 'OK0236', 'FL2287', 'CA4391', 'AK0009', 'NY1397', 'CA4392', 'AK0010', 'FL2288', 'NY1399', 'AK0012', 'CA4396', 'NY1402', 'FL2289', 'AK0013', 'CA4418', 'NY1410', 'FL2301', 'AK0014', 'CA4428', 'NY1413')
					AND a.NET_AMOUNT_RECEIVER <> 0
				GROUP BY
					RTRIM(p.NAME_MAIN_BRANCH),
					RTRIM(co.NAME_COUNTRY),
					a.day
            	""")

			df = df.repartition("day")


			df \
				.write.mode('overwrite') \
				.format('parquet') \
				.partitionBy('day') \
				.save( s3outputpath )

	else:
		print("Input table does not exist, job will exit with the following exception")
		raise Exception("FATAL: Input table, ", input_db, ".", input_table, " does not exist!!! ")
		os._exit()

def create_daily_sales_count_cancelled_v2():
	#Check if target table exists
	input_db='viamericas'
	input_table='receiver'
	target_db='analytics'
	target_table='daily_sales_count_cancelled_v2'
	target_table_exist = wr.catalog.does_table_exist(database=target_db, table=target_table)
	input_table_exist = wr.catalog.does_table_exist(database=input_db, table=input_table)
	s3outputpath = "s3://viamericas-datalake-dev-us-east-1-283731589572-analytics/daily_sales_count_cancelled_v2/"


	if input_table_exist == True:
		#If table exists, just read the last partition, otherwise read the entire table and create the target
		if target_table_exist == True:
			print(f"Target table '{target_table}' exist, non calculated partitions will be created")
			current_target_partitions = list(wr.catalog.get_partitions(database=target_db, table=target_table).values())
			list_current_target_partitions = [f[0] for f in current_target_partitions]
			list_current_target_partitions = list(filter(lambda x: int(x.replace("-","")) > 20201231, list_current_target_partitions))
			list_current_target_partitions.sort(reverse=True)
			current_input_partitions = list(wr.catalog.get_partitions(database=input_db, table=input_table).values())
			list_current_input_partitions = [f[0] for f in current_input_partitions]
			list_current_input_partitions = list(filter(lambda x: int(x.replace("-","")) > 20201231, list_current_input_partitions))
			list_current_input_partitions.sort(reverse=True)
			#list_current_input_partitions = spark.sql(f""" select distinct day from {input_db}.{input_table} """).rdd.map(lambda x: x[0]).collect()
			#list_current_input_partitions.sort(reverse=True)
			days_to_process = list(set(list_current_input_partitions) - set(list_current_target_partitions))
			days_to_process.sort()
			paquete = " OR ".join([str('a.day') + " = " +"'"+str(b)+"'" for b in days_to_process])
			print(" Current target partitions: ", str(list_current_target_partitions))
			print(" Current input partitions: ", str(list_current_input_partitions))
			print(" Days to process: ", str(days_to_process))
			print(" Package expression: ", str(paquete))

			if len(days_to_process) == 0:
				df = spark.sql(f"""
				SELECT
				CAST(RTRIM(p.NAME_MAIN_BRANCH) AS VARCHAR(60)) AS PAYER,
				RTRIM(co.NAME_COUNTRY) AS COUNTRY,
				a.day as DATE,
				SUM(CASE WHEN A.ID_FLAG_RECEIVER = 'A' OR A.ID_FLAG_RECEIVER = 'C' THEN 0 ELSE A.NET_AMOUNT_RECEIVER END) AS AMOUNT,
				SUM(CASE WHEN A.ID_FLAG_RECEIVER = 'A' OR A.ID_FLAG_RECEIVER = 'C' THEN 1 ELSE 0 END) AS TX_CANCELLED,
				a.day
				FROM
				viamericas.RECEIVER a
				INNER JOIN viamericas.GROUP_BRANCH p ON p.ID_MAIN_BRANCH = CASE WHEN a.ID_MAIN_BRANCH_EXPIRED IS NULL THEN RTRIM(a.ID_MAIN_BRANCH_SENT) ELSE RTRIM(a.ID_MAIN_BRANCH_EXPIRED) END
				INNER JOIN viamericas.COUNTRY co ON a.ID_COUNTRY_RECEIVER = co.ID_COUNTRY
				LEFT JOIN viamericas.BRANCH b ON a.ID_BRANCH = b.ID_BRANCH
				WHERE a.day >= '{list_current_input_partitions[0]}' AND
				NOT (A.ID_MAIN_BRANCH_SENT LIKE 'M%')
				AND NOT (A.ID_BRANCH LIKE 'T%')
				AND SUBSTRING(b.id_branch, 1, 1) IN (SELECT SUBSTRING(PREFIX, 1, 1) FROM viamericas.BRANCH_PREFIX P WHERE TRIM(B.ID_COUNTRY) = TRIM(P.ID_COUNTRY))
				AND b.ID_LOCATION IS NOT NULL
				AND b.ID_LOCATION NOT IN ('MD0010', 'MD0952', 'AK0003', 'CA3897', 'NY1130', 'MD0696', 'FL1933', 'AK0004', 'CA4046', 'NY1221', 'MD0623', 'MD1003', 'MD1018', 'AK0008', 'CA4291', 'NY1346', 'CA4350', 'NY1381', 'OK0236', 'FL2287', 'CA4391', 'AK0009', 'NY1397', 'CA4392', 'AK0010', 'FL2288', 'NY1399', 'AK0012', 'CA4396', 'NY1402', 'FL2289', 'AK0013', 'CA4418', 'NY1410', 'FL2301', 'AK0014', 'CA4428', 'NY1413')
				AND a.NET_AMOUNT_RECEIVER <> 0
				GROUP BY
				RTRIM(p.NAME_MAIN_BRANCH),
				RTRIM(co.NAME_COUNTRY),
				a.day
				""")

				df = df.repartition("day")
	

				df \
					.write.mode('overwrite') \
					.format('parquet') \
					.partitionBy('day') \
					.save( s3outputpath )
			if len(days_to_process) > 0 and len(days_to_process) < 20:
				df = spark.sql(f"""
				SELECT
				CAST(RTRIM(p.NAME_MAIN_BRANCH) AS VARCHAR(60)) AS PAYER,
				RTRIM(co.NAME_COUNTRY) AS COUNTRY,
				a.day as DATE,
				SUM(CASE WHEN A.ID_FLAG_RECEIVER = 'A' OR A.ID_FLAG_RECEIVER = 'C' THEN 0 ELSE A.NET_AMOUNT_RECEIVER END) AS AMOUNT,
				SUM(CASE WHEN A.ID_FLAG_RECEIVER = 'A' OR A.ID_FLAG_RECEIVER = 'C' THEN 1 ELSE 0 END) AS TX_CANCELLED,
				a.day
				FROM
				viamericas.RECEIVER a
				INNER JOIN viamericas.GROUP_BRANCH p ON p.ID_MAIN_BRANCH = CASE WHEN a.ID_MAIN_BRANCH_EXPIRED IS NULL THEN RTRIM(a.ID_MAIN_BRANCH_SENT) ELSE RTRIM(a.ID_MAIN_BRANCH_EXPIRED) END
				INNER JOIN viamericas.COUNTRY co ON a.ID_COUNTRY_RECEIVER = co.ID_COUNTRY
				LEFT JOIN viamericas.BRANCH b ON a.ID_BRANCH = b.ID_BRANCH
				WHERE ({paquete}) AND
				NOT (A.ID_MAIN_BRANCH_SENT LIKE 'M%')
				AND NOT (A.ID_BRANCH LIKE 'T%')
				AND SUBSTRING(b.id_branch, 1, 1) IN (SELECT SUBSTRING(PREFIX, 1, 1) FROM viamericas.BRANCH_PREFIX P WHERE TRIM(B.ID_COUNTRY) = TRIM(P.ID_COUNTRY))
				AND b.ID_LOCATION IS NOT NULL
				AND b.ID_LOCATION NOT IN ('MD0010', 'MD0952', 'AK0003', 'CA3897', 'NY1130', 'MD0696', 'FL1933', 'AK0004', 'CA4046', 'NY1221', 'MD0623', 'MD1003', 'MD1018', 'AK0008', 'CA4291', 'NY1346', 'CA4350', 'NY1381', 'OK0236', 'FL2287', 'CA4391', 'AK0009', 'NY1397', 'CA4392', 'AK0010', 'FL2288', 'NY1399', 'AK0012', 'CA4396', 'NY1402', 'FL2289', 'AK0013', 'CA4418', 'NY1410', 'FL2301', 'AK0014', 'CA4428', 'NY1413')
				AND a.NET_AMOUNT_RECEIVER <> 0
				GROUP BY
				RTRIM(p.NAME_MAIN_BRANCH),
				RTRIM(co.NAME_COUNTRY),
				a.day
				""")

				df = df.repartition("day")
	

				df \
					.write.mode('overwrite') \
					.format('parquet') \
					.partitionBy('day') \
					.save( s3outputpath )
			else:
				print('More than 20 partitions missings.')
				df = spark.sql(f"""
					SELECT
					CAST(RTRIM(p.NAME_MAIN_BRANCH) AS VARCHAR(60)) AS PAYER,
					RTRIM(co.NAME_COUNTRY) AS COUNTRY,
					a.day as DATE,
					SUM(CASE WHEN A.ID_FLAG_RECEIVER = 'A' OR A.ID_FLAG_RECEIVER = 'C' THEN 0 ELSE A.NET_AMOUNT_RECEIVER END) AS AMOUNT,
					SUM(CASE WHEN A.ID_FLAG_RECEIVER = 'A' OR A.ID_FLAG_RECEIVER = 'C' THEN 1 ELSE 0 END) AS TX_CANCELLED,
					a.day
					FROM
					viamericas.RECEIVER a
					INNER JOIN viamericas.GROUP_BRANCH p ON p.ID_MAIN_BRANCH = CASE WHEN a.ID_MAIN_BRANCH_EXPIRED IS NULL THEN RTRIM(a.ID_MAIN_BRANCH_SENT) ELSE RTRIM(a.ID_MAIN_BRANCH_EXPIRED) END
					INNER JOIN viamericas.COUNTRY co ON a.ID_COUNTRY_RECEIVER = co.ID_COUNTRY
					LEFT JOIN viamericas.BRANCH b ON a.ID_BRANCH = b.ID_BRANCH
					WHERE
					NOT (A.ID_MAIN_BRANCH_SENT LIKE 'M%')
					AND NOT (A.ID_BRANCH LIKE 'T%')
					AND SUBSTRING(b.id_branch, 1, 1) IN (SELECT SUBSTRING(PREFIX, 1, 1) FROM viamericas.BRANCH_PREFIX P WHERE TRIM(B.ID_COUNTRY) = TRIM(P.ID_COUNTRY))
					AND a.day >= '{days_to_process[0]}'
					AND b.ID_LOCATION IS NOT NULL
					AND b.ID_LOCATION NOT IN ('MD0010', 'MD0952', 'AK0003', 'CA3897', 'NY1130', 'MD0696', 'FL1933', 'AK0004', 'CA4046', 'NY1221', 'MD0623', 'MD1003', 'MD1018', 'AK0008', 'CA4291', 'NY1346', 'CA4350', 'NY1381', 'OK0236', 'FL2287', 'CA4391', 'AK0009', 'NY1397', 'CA4392', 'AK0010', 'FL2288', 'NY1399', 'AK0012', 'CA4396', 'NY1402', 'FL2289', 'AK0013', 'CA4418', 'NY1410', 'FL2301', 'AK0014', 'CA4428', 'NY1413')
					AND a.NET_AMOUNT_RECEIVER <> 0
					GROUP BY
					RTRIM(p.NAME_MAIN_BRANCH),
					RTRIM(co.NAME_COUNTRY),
					a.day
					""")

				df = df.repartition("day")


				df \
					.write.mode('overwrite') \
					.format('parquet') \
					.partitionBy('day') \
					.save( s3outputpath )
		else:
			print("Target table does not exist, will be created")
			df = spark.sql(f"""
				SELECT
				CAST(RTRIM(p.NAME_MAIN_BRANCH) AS VARCHAR(60)) AS PAYER,
				RTRIM(co.NAME_COUNTRY) AS COUNTRY,
				a.day as DATE,
				SUM(CASE WHEN A.ID_FLAG_RECEIVER = 'A' OR A.ID_FLAG_RECEIVER = 'C' THEN 0 ELSE A.NET_AMOUNT_RECEIVER END) AS AMOUNT,
				SUM(CASE WHEN A.ID_FLAG_RECEIVER = 'A' OR A.ID_FLAG_RECEIVER = 'C' THEN 1 ELSE 0 END) AS TX_CANCELLED,
				a.day
				FROM
				viamericas.RECEIVER a
				INNER JOIN viamericas.GROUP_BRANCH p ON p.ID_MAIN_BRANCH = CASE WHEN a.ID_MAIN_BRANCH_EXPIRED IS NULL THEN RTRIM(a.ID_MAIN_BRANCH_SENT) ELSE RTRIM(a.ID_MAIN_BRANCH_EXPIRED) END
				INNER JOIN viamericas.COUNTRY co ON a.ID_COUNTRY_RECEIVER = co.ID_COUNTRY
				LEFT JOIN viamericas.BRANCH b ON a.ID_BRANCH = b.ID_BRANCH
				WHERE
				NOT (A.ID_MAIN_BRANCH_SENT LIKE 'M%')
				AND NOT (A.ID_BRANCH LIKE 'T%')
				AND SUBSTRING(b.id_branch, 1, 1) IN (SELECT SUBSTRING(PREFIX, 1, 1) FROM viamericas.BRANCH_PREFIX P WHERE TRIM(B.ID_COUNTRY) = TRIM(P.ID_COUNTRY))
				AND a.day >= '2021-01-01'
				AND b.ID_LOCATION IS NOT NULL
				AND b.ID_LOCATION NOT IN ('MD0010', 'MD0952', 'AK0003', 'CA3897', 'NY1130', 'MD0696', 'FL1933', 'AK0004', 'CA4046', 'NY1221', 'MD0623', 'MD1003', 'MD1018', 'AK0008', 'CA4291', 'NY1346', 'CA4350', 'NY1381', 'OK0236', 'FL2287', 'CA4391', 'AK0009', 'NY1397', 'CA4392', 'AK0010', 'FL2288', 'NY1399', 'AK0012', 'CA4396', 'NY1402', 'FL2289', 'AK0013', 'CA4418', 'NY1410', 'FL2301', 'AK0014', 'CA4428', 'NY1413')
				AND a.NET_AMOUNT_RECEIVER <> 0
				GROUP BY
				RTRIM(p.NAME_MAIN_BRANCH),
				RTRIM(co.NAME_COUNTRY),
				a.day
				""")

			df = df.repartition("day")


			df \
				.write.mode('overwrite') \
				.format('parquet') \
				.partitionBy('day') \
				.save( s3outputpath )

	else:
		print("Input table does not exist, job will exit with the following exception")
		raise Exception("FATAL: Input table, ", input_db, ".", input_table, " does not exist!!! ")
		os._exit()		

#create_daily_check()
create_last_daily_forex()
create_daily_check_gp()
create_daily_sales_count_cancelled_v2()
job.commit()

########################## ABT ##########################
'''
Inputs: 
I. analytics.daily_check 
Cambios:
1. date a datetime
2. Construir columna payer_country > concat(payer, "_", country)
3. Filtro: date > '2020-12-31 23:59:59'

Dentro de la función aging_filter >>>
	4. Tomar día más reciente de date, crear limit_date como un día anterior a last_date_sample
	5. Crea tabla intermedia result, con min(date), max(date), sum(amount) y sum(tx) group by payer_country
	6. Agrega a result:
		- result['age_payer'] = ((limit_date - result['first_date']).dt.days / 30).round(2) >>>> Parece ser un redondeo de cantidad de meses de 30 días
		- result['active_time'] = ((result['last_date'] - result['first_date']).dt.days / 30).round(2) >>>> Parece ser un redondeo de cantidad de meses de 30 días pero tomando el last_date del payer_country
		- result['inactive_time'] = ((limit_date - result['last_date']).dt.days / 30).round(2)>>>> Idem
	7. Create el dataframe aging_universe del siguiente modo:
	aging_universe = result.loc[
			(result.age_payer >= 3) & 
			(result.inactive_time <= 3) & 
			(result.total_amount > 10000) & 
			(result.total_transactions > 50)

8. Ver qué intenta hacer acá
df_aging = aging_filter(df) #Filtering 'payer_country' based on Aging notebook
df_filtered = df[df['payer_country'].isin(df_aging['payer_country'])] # Applying aging filters 
df_filtered['date'] = pd.to_datetime(df_filtered['date']).dt.date
OUTPUT FINAL >>> df_filtered

II. analytics.last_daily_forex
1. Renombra variables después de un select *
df_rates=df_rates.rename(columns={'day': 'date', 'max_feed_price': 'feed_price'})
df_rates=df_rates.loc[:,['date', 'feed_price', 'symbol']]
2. Mete dentro de la función get_closing_prices:
	- Castea date a datetime
	- Filtra por fechas entre 2021-01-01 y 2023-10-22 (Ver cuál es la idea en un job automatizado)
	- Group by por symbol y feed_date, para quedarse con el last_value de feed_price (lo que ya hace el job de last_daily_forex)
3. Mete dentro de la función generate_lag_and_variation:
	- Genera variables rate_lag_{n} que es simplemente el lag(n) de feed_price
	- Genera variables var_rate_lag_{n} que es simplemente la diferencia de  lag(n) y lag(n+1) de feed_price
	- Genera 14 variables de lag y 13 de diferencias de lags

III. Dict de rates > Mapea symbol con país > Se puede incluir en la misma tabla de rates
rates_dict = {
    'USDBRL': 'BRAZIL', # Bz Real 
    'USDINR': 'INDIA', # Indian Rupia
    'USDGTQ': 'GUATEMALA', #Quetzal 
    'USDMXN': 'MEXICO', #Mx Peso
    'USDPHP': 'PHILIPPINES' # Ph Peso
}
OUTPUT FINAL >>> rates

IV. LEFT Join de ambos outputs por date y country (df1)

V. Efecto de transacciones canceladas >>>> analytics.daily_sales_count_cancelled_v2
Cambios:
1. date a datetime
2. Filtro: date > '2020-12-31 23:59:59'
3. Construir columna payer_country > concat(payer, "_", country)
4. Usa labelencoder de sklearn para codificar valores de payer_country
label_encoder = LabelEncoder()
# Coding ‘PAYER_COUNTRY’ as unique values
df2['payer_country_encoder'] = label_encoder.fit_transform(df2['payer_country'])
# By applying the same aging filter, we can work on the same payer_country universe 
df2 = df2[df2['payer_country'].isin(df_aging['payer_country'])]
5. Función fill_missing_dates, crea para el dataset anterior (df2) con el producto de fechas totales para rellenar las combinaciones vacías de amount y tx_cancelled (chequear)
6. Función generate_tx_lags_and_variation, similar a la función de lag anterior agrupando el lag por country y payer para tx_cancelled y la diferencia entre lag(n) y lag(n+1)
OUTPUT FINAL > df2

VII. df_final
1. Merge de df1 y df2 (full join) por 'date','payer','country', 'amount'
2. Deberían quedar las siguientes 25 columnas 

#   Column                  Non-Null Count   Dtype         
---  ------                  --------------   -----         
 0   date                    133120 non-null  datetime64[ns]
 1   payer                   133120 non-null  object        
 2   country                 133120 non-null  object        
 3   amount                  133120 non-null  float64       
 4   var_rate_lag_1          33730 non-null   float64       
 5   var_rate_lag_2          33701 non-null   float64       
 6   var_rate_lag_3          33673 non-null   float64       
 7   var_rate_lag_4          33644 non-null   float64       
 8   var_rate_lag_5          33617 non-null   float64       
 9   var_rate_lag_6          33589 non-null   float64       
 10  var_rate_lag_7          33561 non-null   float64       
 11  var_rate_lag_8          33534 non-null   float64       
 12  var_rate_lag_9          33505 non-null   float64       
 13  var_rate_lag_10         33477 non-null   float64       
 14  var_rate_lag_11         33448 non-null   float64       
 15  var_rate_lag_12         33420 non-null   float64       
 16  var_rate_lag_13         33392 non-null   float64       
 17  payer_country_encoder   133120 non-null  float64       
 18  payer_country           133120 non-null  object        
 19  var_tx_cancelled_lag_1  132860 non-null  float64       
 20  var_tx_cancelled_lag_2  132730 non-null  float64       
 21  var_tx_cancelled_lag_3  132600 non-null  float64       
 22  var_tx_cancelled_lag_4  132470 non-null  float64       
 23  var_tx_cancelled_lag_5  132340 non-null  float64       
 24  var_tx_cancelled_lag_6  132210 non-null  float64 

3. Agregar un flag de weekend (ver si está contando el viernes también)
4. Agrega un flag de holidays llamado special_dates (ver si está ok ya que algunas fechas corresponden solo a países específicos pero en el código está aplicando a todos)
5. Imputa 0 a todos los missings (ver si está ok, imputa a todas las variables de lags)
6. Vuelve a generar la variable payer_country_encoder (ver si está ok, parece ser que solo se hizo para verificar que el código sea el mismo)

VIII. Genera 3 salidas en csv (ver lo que se debería filtrar, hay un ejemplo que filtra < '2023-07-01' para prueba)

1. Target > Se queda con 'payer_country_encoder', 'date', 'amount' y los renombra como item_id, timestamp y target_value
2. RELATED_TS > Se queda con 'payer_country_encoder', 'date', 
                      'var_rate_lag_1', 'var_rate_lag_2', 'var_rate_lag_3', 'var_rate_lag_4', 'var_rate_lag_5',
       'var_rate_lag_6', 'var_rate_lag_7', 'var_rate_lag_8', 'var_rate_lag_9', 'var_rate_lag_10', 'var_rate_lag_11', 'var_rate_lag_12',
       'var_rate_lag_13', 'var_tx_cancelled_lag_1', 'var_tx_cancelled_lag_2',
       'var_tx_cancelled_lag_3', 'var_tx_cancelled_lag_4', 'var_tx_cancelled_lag_5', 'var_tx_cancelled_lag_6', 'weekend', 'special_dates' y renombra payer_country_encoder por item_id y date por timestamp


'''