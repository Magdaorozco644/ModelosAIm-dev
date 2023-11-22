
import boto3, json
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_date

# Contexto
sc = SparkContext()
spark = SparkSession(sc)
glueContext = GlueContext(spark)

spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")

def get_secret(secret_name, region_name):
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e
    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response['SecretString']
    secret_=json.loads(secret)
    return secret_

# Obtener credenciales para SQL Server
secret_name = "SQLSERVER-CREDENTIALS"
region_name = "us-east-1"
secret = get_secret(secret_name, region_name)

jdbc_viamericas = "jdbc:sqlserver://172.17.13.45:1433;database=Envio"
qryStr = f"(SELECT [MAINLINEOFBUSINESSID] ,[HISTORYQUESTIONB] ,[ISCOSIGNER] ,[DRIVERSLICENSEOWNER] ,[PREAPPROVALOLDID] ,[BUSINESSLEGALNAME] ,[RENT] ,[ADDRESS] ,[COMPLIANCEOFFICER] ,[CONTRACTVERSION] ,[ACHIDTYPE] ,[TYPEAPPLICATIONID] ,[FREQUENCY] ,[NAMEOFBANK] ,[COUNTY] ,[IDSTATEINCORPORATION] ,[TELEPHONE] ,[DAILYCREDITLIMITREQUESTED] ,[DATEINCORPORATION] ,[OWNER_COUNTRY] ,[ACHACCOUNTNUMBER] ,[CITY] ,[INSURANCECOMPANY] ,[AUTH_EXPERIAN_CREDIT_REPORT] ,[PRINCIPALCONTACT] ,[HISTORYQUESTIONA] ,[PRIORNAMEORADDRESSBUSINESS] ,[FEDERALPAYERIDNUMBER] ,[HISTORYQUESTIOND] ,[LIABILITY] ,[ESTIMATEDVALUESHIPMENTSDAILY] ,[TIME_BUSINESS] ,[JustificationOfQuota] ,[ACHROUTING] ,[TELEPHONEOWNER] ,[StateProcess] ,[ISREADY] ,[HISTORYQUESTIONE] ,[DATEPROCESSED] ,[ID_BRANCH] ,[EXPIRATIONDATE] ,[ID_BRANCH_RP] ,[DETAILSHISTORYQUESTIONC] ,[OWNERSHIPOWNER] ,[DATEOFBIRTHOWNER] ,[ACHFREQUENCY] ,[applicationoldid] ,[IPROCESSEDPREAPP] ,[BUSINESSPROFILEID] ,[IDCITY] ,[GENDER_OWNER] ,[HoursOfOperation] ,[ID_BANCO] ,[IDSTATE] ,[IdCountryTopOne] ,[DETAILSHISTORYQUESTIONB] ,[ZIPECODEOWNER] ,[ANSWERREQUIREDCOSIGNER] ,[ESTIMATEDNUMBERTOSENT] ,[PRINCIPALOWNERNAME] ,[EmailToSend] ,[SOCIALSECURITYNUMBEROWNER] ,[POLICYNUMBER] ,[DETAILSHISTORYQUESTIOND] ,[IDSTATEOWNER] ,[OTHERAGENCIESOWNER] ,[CREDITAPPROVAL] ,[URLFILEPDF] ,[OTHERMAINLINEOFBUSINESS] ,[BUSINESSREFERENCENAMEANDPHONE] ,[HOMEADDRESS] ,[OWNERIDNUMBER] ,[CREDITAPPROVALNOTE] ,[OTHERAGENTSOFFICER] ,[SPOUSENAMEOWNER] ,[SIGNATUREDATE] ,[IDSTATEDIRVERSLICENSEOWNER] ,[ANSWERSENDEMAIL] ,[IdCountryTopThree] ,[ID_CUENTA_BANCO] ,[ROBBERY] ,[ACCOUNTNUMBER] ,[ZIPCODE] ,[FAX] ,[METHODSOFPAYMENTID] ,[ACHINSTITUTION] ,[ACHACCOUNTTYPE] ,[PERSONALREFERENCENAMEANDPHONE] ,[HOWLONGHAVEYOUBEENINBUSINESS] ,[BUSINESSGROSSMONTLYSALES] ,[DateLastUpdate] ,[EMAILBUSINESS] ,[EMAILOWNERS] ,[BANKTELEPHONE] ,[DETAILSHISTORYQUESTIONE] ,[OWNER_CITY_NAME] ,[DETAILSHISTORYQUESTIONA] ,[DateRegister] ,[EMAILOWNER] ,[SINGLEORMARRIED] ,[APPLICATIONID] ,[BUSINESSDBANAME] ,[LOGIN] ,[TYPEOFID] ,[MasterStatusId] ,[TAXID] ,[ACHACCOUNTTITLE] ,[OWN] ,[MIDDLENAME] ,[DATEREQUEST] ,[IdCountryTopTwo] ,[HISTORYQUESTIONC] ,[TYPEOFTAXID] ,[BANKCONTACT] ,[IPROCESSED] ,[OWNERLASTNAME] ,[IDCITYOWNER] ,[ACHIDASSOCIATED] FROM envio.dba.vapp_application) x"

jdbcDF = spark.read.format('jdbc')\
        .option('url', jdbc_viamericas)\
        .option('driver', 'com.microsoft.sqlserver.jdbc.SQLServerDriver')\
        .option('dbtable', qryStr )\
        .option("user", secret['username'])\
        .option("password", secret['password'])\
        .option("numPartitions", 10)\
        .option("fetchsize", 1000)\
        .load()

# Definir la ruta de salida en S3
s3_output_path = f"s3://viamericas-datalake-dev-us-east-1-283731589572-raw/envio/dba/vapp_application/"

# Escribir el DataFrame en formato Parquet en S3
jdbcDF.write.parquet(s3_output_path, mode="overwrite")
    