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
			#id_main_branch , id_country
			if len(days_to_process) == 0:
				df = spark.sql(f"""
                select distinct feed_tbl.symbol, feed_tbl.max_feed_date, feed_tbl.max_feed_price, c.name_country as country, feed_tbl.day as day, tg.id_country
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
			elif len(days_to_process) > 0 and len(days_to_process) < 20:
				df = spark.sql(f"""
                select distinct feed_tbl.symbol, feed_tbl.max_feed_date, feed_tbl.max_feed_price, c.name_country as country, feed_tbl.day as day, tg.id_country
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
				select distinct feed_tbl.symbol, feed_tbl.max_feed_date, feed_tbl.max_feed_price, c.name_country as country, feed_tbl.day as day, tg.id_country
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
            select distinct feed_tbl.symbol, feed_tbl.max_feed_date, feed_tbl.max_feed_price, c.name_country as country, feed_tbl.day as day, tg.id_country
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
					a.day as day,
					p.ID_MAIN_BRANCH,
					co.ID_COUNTRY
				FROM
					viamericas.RECEIVER a
				INNER JOIN viamericas.GROUP_BRANCH p ON p.ID_MAIN_BRANCH = CASE WHEN a.ID_MAIN_BRANCH_EXPIRED IS NULL THEN RTRIM(a.ID_MAIN_BRANCH_SENT) ELSE RTRIM(a.ID_MAIN_BRANCH_EXPIRED) END
				INNER JOIN viamericas.COUNTRY co ON a.ID_COUNTRY_RECEIVER = co.ID_COUNTRY
				LEFT JOIN viamericas.RECEIVER_GP_COMPONENTS vd ON a.ID_BRANCH = vd.ID_BRANCH and a.ID_RECEIVER = vd.ID_RECEIVER
				WHERE
    				a.day = '{list_current_input_partitions[0]}' AND
					NOT (A.ID_MAIN_BRANCH_SENT LIKE 'M%')
					AND NOT (A.ID_BRANCH LIKE 'T%')
					AND a.NET_AMOUNT_RECEIVER <> 0
     				AND a.ID_BRANCH NOT IN ('A00025','A00026','A00027','A00028','A00029','A00033','A00043','A00047','A00048',
         									'A00049','A00051','A00052','A00053','A00054','A00055','A00056','A00057','A00058',
                  							'A00059','A00061','A00062','A00072','A00073','A00074','A00075','A00076','A00077',
                         					'A00078','A00079','A00081','A00082','A00083','A00084','A00086','A00087','A00088',
                              				'R00025','R00043')
				GROUP BY
					RTRIM(p.NAME_MAIN_BRANCH),
					RTRIM(co.NAME_COUNTRY),
					p.ID_MAIN_BRANCH,
					co.ID_COUNTRY,
					a.day;
				""")

				df = df.repartition("day")
				print("Total rows >>>>>>>>>>>> ", df.count())


				df \
					.write.mode('overwrite') \
					.format('parquet') \
					.partitionBy('day') \
					.save( s3outputpath )
			elif len(days_to_process) > 0 and len(days_to_process) < 20:
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
					a.day as day,
					p.ID_MAIN_BRANCH,
					co.ID_COUNTRY
				FROM
					viamericas.RECEIVER a
				INNER JOIN viamericas.GROUP_BRANCH p ON p.ID_MAIN_BRANCH = CASE WHEN a.ID_MAIN_BRANCH_EXPIRED IS NULL THEN RTRIM(a.ID_MAIN_BRANCH_SENT) ELSE RTRIM(a.ID_MAIN_BRANCH_EXPIRED) END
				INNER JOIN viamericas.COUNTRY co ON a.ID_COUNTRY_RECEIVER = co.ID_COUNTRY
				LEFT JOIN viamericas.RECEIVER_GP_COMPONENTS vd ON a.ID_BRANCH = vd.ID_BRANCH and a.ID_RECEIVER = vd.ID_RECEIVER
				WHERE
    				({paquete}) AND
					NOT (A.ID_MAIN_BRANCH_SENT LIKE 'M%')
					AND NOT (A.ID_BRANCH LIKE 'T%')
					AND a.NET_AMOUNT_RECEIVER <> 0
     				AND a.ID_BRANCH NOT IN ('A00025','A00026','A00027','A00028','A00029','A00033','A00043','A00047','A00048',
         									'A00049','A00051','A00052','A00053','A00054','A00055','A00056','A00057','A00058',
                  							'A00059','A00061','A00062','A00072','A00073','A00074','A00075','A00076','A00077',
                         					'A00078','A00079','A00081','A00082','A00083','A00084','A00086','A00087','A00088',
                              				'R00025','R00043')
				GROUP BY
					RTRIM(p.NAME_MAIN_BRANCH),
					RTRIM(co.NAME_COUNTRY),
					p.ID_MAIN_BRANCH,
					co.ID_COUNTRY,
					a.day;
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
						a.day as day,
						p.ID_MAIN_BRANCH,
						co.ID_COUNTRY
					FROM
						viamericas.RECEIVER a
					INNER JOIN viamericas.GROUP_BRANCH p ON p.ID_MAIN_BRANCH =  CASE WHEN a.ID_MAIN_BRANCH_EXPIRED IS NULL THEN RTRIM(a.ID_MAIN_BRANCH_SENT) ELSE RTRIM(a.ID_MAIN_BRANCH_EXPIRED) END
					INNER JOIN viamericas.COUNTRY co ON a.ID_COUNTRY_RECEIVER = co.ID_COUNTRY
					LEFT JOIN viamericas.RECEIVER_GP_COMPONENTS vd ON a.ID_BRANCH = vd.ID_BRANCH and a.ID_RECEIVER = vd.ID_RECEIVER
					WHERE
						NOT (A.ID_MAIN_BRANCH_SENT LIKE 'M%')
						AND NOT (A.ID_BRANCH LIKE 'T%')
						AND a.day >= '{days_to_process[0]}'
						AND a.NET_AMOUNT_RECEIVER <> 0
           				AND a.ID_BRANCH NOT IN ('A00025','A00026','A00027','A00028','A00029','A00033','A00043','A00047','A00048',
         									'A00049','A00051','A00052','A00053','A00054','A00055','A00056','A00057','A00058',
                  							'A00059','A00061','A00062','A00072','A00073','A00074','A00075','A00076','A00077',
                         					'A00078','A00079','A00081','A00082','A00083','A00084','A00086','A00087','A00088',
                              				'R00025','R00043')
					GROUP BY
						RTRIM(p.NAME_MAIN_BRANCH),
						RTRIM(co.NAME_COUNTRY),
						p.ID_MAIN_BRANCH,
						co.ID_COUNTRY,
						a.day;
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
					a.day as day,
					p.ID_MAIN_BRANCH,
					co.ID_COUNTRY
				FROM
					viamericas.RECEIVER a
				INNER JOIN viamericas.GROUP_BRANCH p ON p.ID_MAIN_BRANCH =  CASE WHEN a.ID_MAIN_BRANCH_EXPIRED IS NULL THEN RTRIM(a.ID_MAIN_BRANCH_SENT) ELSE RTRIM(a.ID_MAIN_BRANCH_EXPIRED) END
				INNER JOIN viamericas.COUNTRY co ON a.ID_COUNTRY_RECEIVER = co.ID_COUNTRY
				LEFT JOIN viamericas.RECEIVER_GP_COMPONENTS vd ON a.ID_BRANCH = vd.ID_BRANCH and a.ID_RECEIVER = vd.ID_RECEIVER
				WHERE
					NOT (A.ID_MAIN_BRANCH_SENT LIKE 'M%')
					AND NOT (A.ID_BRANCH LIKE 'T%')
					AND a.day >= '2021-01-01'
					AND a.NET_AMOUNT_RECEIVER <> 0
          			AND a.ID_BRANCH NOT IN ('A00025','A00026','A00027','A00028','A00029','A00033','A00043','A00047','A00048',
         									'A00049','A00051','A00052','A00053','A00054','A00055','A00056','A00057','A00058',
                  							'A00059','A00061','A00062','A00072','A00073','A00074','A00075','A00076','A00077',
                         					'A00078','A00079','A00081','A00082','A00083','A00084','A00086','A00087','A00088',
                              				'R00025','R00043')
				GROUP BY
					RTRIM(p.NAME_MAIN_BRANCH),
					RTRIM(co.NAME_COUNTRY),
					p.ID_MAIN_BRANCH,
					co.ID_COUNTRY,
					a.day;
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
				WHERE a.day >= '{list_current_input_partitions[0]}' AND
				NOT (A.ID_MAIN_BRANCH_SENT LIKE 'M%')
				AND NOT (A.ID_BRANCH LIKE 'T%')
				AND a.ID_BRANCH NOT IN ('A00025','A00026','A00027','A00028','A00029','A00033','A00043','A00047','A00048',
											'A00049','A00051','A00052','A00053','A00054','A00055','A00056','A00057','A00058',
											'A00059','A00061','A00062','A00072','A00073','A00074','A00075','A00076','A00077',
											'A00078','A00079','A00081','A00082','A00083','A00084','A00086','A00087','A00088',
											'R00025','R00043')
        		AND a.NET_AMOUNT_RECEIVER <> 0
				GROUP BY
				RTRIM(p.NAME_MAIN_BRANCH),
				RTRIM(co.NAME_COUNTRY),
				a.day
				""")

				df = df.repartition("day")
				print("Total rows >>>>>>>>>>>> ", df.count())

				df \
					.write.mode('overwrite') \
					.format('parquet') \
					.partitionBy('day') \
					.save( s3outputpath )
			elif len(days_to_process) > 0 and len(days_to_process) < 20:
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
				WHERE ({paquete}) AND
				NOT (A.ID_MAIN_BRANCH_SENT LIKE 'M%')
				AND NOT (A.ID_BRANCH LIKE 'T%')
				AND a.ID_BRANCH NOT IN ('A00025','A00026','A00027','A00028','A00029','A00033','A00043','A00047','A00048',
											'A00049','A00051','A00052','A00053','A00054','A00055','A00056','A00057','A00058',
											'A00059','A00061','A00062','A00072','A00073','A00074','A00075','A00076','A00077',
											'A00078','A00079','A00081','A00082','A00083','A00084','A00086','A00087','A00088',
											'R00025','R00043')
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
					WHERE
					a.day >= '{days_to_process[0]}'
					AND NOT (A.ID_MAIN_BRANCH_SENT LIKE 'M%')
					AND NOT (A.ID_BRANCH LIKE 'T%')
					AND a.ID_BRANCH NOT IN ('A00025','A00026','A00027','A00028','A00029','A00033','A00043','A00047','A00048',
												'A00049','A00051','A00052','A00053','A00054','A00055','A00056','A00057','A00058',
												'A00059','A00061','A00062','A00072','A00073','A00074','A00075','A00076','A00077',
												'A00078','A00079','A00081','A00082','A00083','A00084','A00086','A00087','A00088',
												'R00025','R00043')
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
				WHERE
				a.day >= '2021-01-01'
				AND NOT (A.ID_MAIN_BRANCH_SENT LIKE 'M%')
				AND NOT (A.ID_BRANCH LIKE 'T%')
     			AND a.ID_BRANCH NOT IN ('A00025','A00026','A00027','A00028','A00029','A00033','A00043','A00047','A00048',
         									'A00049','A00051','A00052','A00053','A00054','A00055','A00056','A00057','A00058',
                  							'A00059','A00061','A00062','A00072','A00073','A00074','A00075','A00076','A00077',
                         					'A00078','A00079','A00081','A00082','A00083','A00084','A00086','A00087','A00088',
                              				'R00025','R00043')
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
