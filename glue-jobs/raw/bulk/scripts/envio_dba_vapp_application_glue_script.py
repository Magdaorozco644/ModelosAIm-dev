
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
glueContext.setTempDir("s3://viamericas-datalake-dev-us-east-1-283731589572-athena/gluetmp/")


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
qryStr = f"(SELECT [PERSONALREFERENCENAMEANDPHONE] ,[LOGIN] ,[ACHACCOUNTTYPE] ,[COUNTY] ,[OWNER_CITY_NAME] ,[BUSINESSGROSSMONTLYSALES] ,[CREDITAPPROVALNOTE] ,[IDCITY] ,[LIABILITY] ,[ACHFREQUENCY] ,[ACHINSTITUTION] ,[IDSTATEDIRVERSLICENSEOWNER] ,[OWNERIDNUMBER] ,[EmailToSend] ,[IDCITYOWNER] ,[PRINCIPALCONTACT] ,[SINGLEORMARRIED] ,[DATEINCORPORATION] ,[TYPEAPPLICATIONID] ,[ANSWERSENDEMAIL] ,[OTHERAGENCIESOWNER] ,[ADDRESS] ,[ID_BRANCH_RP] ,[BUSINESSLEGALNAME] ,[BANKTELEPHONE] ,[TELEPHONEOWNER] ,[URLFILEPDF] ,[ACHACCOUNTNUMBER] ,[HISTORYQUESTIONB] ,[DATEPROCESSED] ,[TYPEOFID] ,[ZIPECODEOWNER] ,[HISTORYQUESTIONC] ,[BUSINESSPROFILEID] ,[OWNERSHIPOWNER] ,[IPROCESSEDPREAPP] ,[ZIPCODE] ,[EMAILOWNER] ,[FEDERALPAYERIDNUMBER] ,[DETAILSHISTORYQUESTIONA] ,[DATEREQUEST] ,[ACHIDTYPE] ,[PREAPPROVALOLDID] ,[HISTORYQUESTIONA] ,[ANSWERREQUIREDCOSIGNER] ,[CONTRACTVERSION] ,[IdCountryTopTwo] ,[DATEOFBIRTHOWNER] ,[MIDDLENAME] ,[TIME_BUSINESS] ,[EXPIRATIONDATE] ,[TAXID] ,[OWN] ,[DETAILSHISTORYQUESTIONC] ,[OTHERMAINLINEOFBUSINESS] ,[AUTH_EXPERIAN_CREDIT_REPORT] ,[INSURANCECOMPANY] ,[HISTORYQUESTIOND] ,[IDSTATEOWNER] ,[DETAILSHISTORYQUESTIONE] ,[APPLICATIONID] ,[TELEPHONE] ,[ACHACCOUNTTITLE] ,[SIGNATUREDATE] ,[FREQUENCY] ,[GENDER_OWNER] ,[HoursOfOperation] ,[CREDITAPPROVAL] ,[ACHROUTING] ,[IDSTATE] ,[DETAILSHISTORYQUESTIOND] ,[DateRegister] ,[ACHIDASSOCIATED] ,[ID_BANCO] ,[StateProcess] ,[ISREADY] ,[OWNERLASTNAME] ,[CITY] ,[DAILYCREDITLIMITREQUESTED] ,[BUSINESSREFERENCENAMEANDPHONE] ,[ID_CUENTA_BANCO] ,[IPROCESSED] ,[ESTIMATEDVALUESHIPMENTSDAILY] ,[OTHERAGENTSOFFICER] ,[ID_BRANCH] ,[NAMEOFBANK] ,[FAX] ,[DateLastUpdate] ,[JustificationOfQuota] ,[MAINLINEOFBUSINESSID] ,[EMAILBUSINESS] ,[SOCIALSECURITYNUMBEROWNER] ,[HOWLONGHAVEYOUBEENINBUSINESS] ,[IDSTATEINCORPORATION] ,[HISTORYQUESTIONE] ,[ISCOSIGNER] ,[EMAILOWNERS] ,[COMPLIANCEOFFICER] ,[IdCountryTopThree] ,[PRINCIPALOWNERNAME] ,[applicationoldid] ,[RENT] ,[PRIORNAMEORADDRESSBUSINESS] ,[ESTIMATEDNUMBERTOSENT] ,[MasterStatusId] ,[POLICYNUMBER] ,[ACCOUNTNUMBER] ,[SPOUSENAMEOWNER] ,[HOMEADDRESS] ,[BUSINESSDBANAME] ,[DETAILSHISTORYQUESTIONB] ,[OWNER_COUNTRY] ,[TYPEOFTAXID] ,[DRIVERSLICENSEOWNER] ,[IdCountryTopOne] ,[BANKCONTACT] ,[ROBBERY] ,[METHODSOFPAYMENTID] FROM envio.dba.vapp_application) x"

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
    