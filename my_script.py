from pyspark.sql import SparkSession
from pyspark.sql.functions import col, initcap
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType,StringType,FloatType,DoubleType
import shutil
import os

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Referral Data Processing") \
    .master("local[*]") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

file_paths = {
    "user_referrals": "dataset/user_referrals.csv",
    "user_referral_logs": "dataset/user_referral_logs.csv",
    "user_logs": "dataset/user_logs.csv",
    "user_referral_statuses": "dataset/user_referral_statuses.csv",
    "referral_rewards": "dataset/referral_rewards.csv",
    "paid_transactions": "dataset/paid_transactions.csv",
    "lead_logs": "dataset/lead_logs.csv"
}


# Task 1: load csv dari dataset
def load_csv(file_paths: dict) -> dict:
    """
    Load CSV files into a dictionary of DataFrames.
    """
    spark.conf.set("spark.sql.session.timeZone", "UTC")    
    dataframes = {}
    for table_name, file_path in file_paths.items():
        dataframes[table_name] = spark.read.csv(file_path, header=True, inferSchema=True)
    return dataframes

# Task 2: Cleaning dataset dari missing values
def data_cleansing(dataframes: dict) -> dict:
    """
    Clean data by dropping rows with missing values.
    """
    cleaned_dataframes = {}
    for table_name, df in dataframes.items():
        cleaned_dataframes[table_name] = df.dropna()
    return cleaned_dataframes



def process_data(dataframes: dict) -> DataFrame:
    #user_logs = dedupChecking('user_logs')
    #user_referrals = dedupChecking('user_referrals')
    #lead_logs = dedupChecking('lead_logs')
    #user_referral_logs = dedupChecking('user_referral_logs')
    #referral_rewards = dedupChecking('referral_rewards')
    #user_referral_statuses = dedupChecking('user_referral_statuses')

    def dedupChecking(dataframesAll,Tablename):
        if Tablename != "user_referrals":
            df = dataframesAll[Tablename].alias(Tablename)
            df = df.dropDuplicates([get_join_column(Tablename,df)[0]])
        else:
            df = dataframesAll[Tablename].alias(Tablename)
            df = df.dropDuplicates()
        return df
        
    def get_join_column(table_name,dataframe):
        referral_columns = {
            "lead_logs": ("lead_id","referee_id"),
            "user_logs": ("user_id","referrer_id"),
            "user_referral_logs": ("user_referral_id","referral_id"),
            "paid_transactions": ("transaction_id","transaction_id"),
            "referral_rewards": ("id","referral_reward_id"),
            "user_referral_statuses": ("id","user_referral_status_id")
        }
        if table_name in referral_columns:
            return referral_columns[table_name]
        return None 

    def checkTimezone(table_name, tabletarget):
        user_logs = dataframes['user_logs'].alias("user_logs")
        user_logs = user_logs.dropDuplicates(["user_id"]) # Deduplicate user_logs based on user_id
        user_referrals = dataframes['user_referrals'].alias("user_referrals")
        if "timezone_homeclub" in tabletarget.columns:
            return tabletarget
        elif table_name == "user_referrals":
            merged_df = user_referrals.join(
                user_logs,
                user_referrals.referrer_id == user_logs.user_id,
                "left"
            )
            return merged_df.select(tabletarget["*"], user_logs["timezone_homeclub"].alias("timezone_homeclub"))
        else:
            join_column = get_join_column(table_name,tabletarget)
            if join_column:
                merged_df = tabletarget.join(
                    user_referrals,
                    tabletarget[join_column[0]] == user_referrals[join_column[1]],
                    "left"
                ).join(
                    user_logs,
                    user_referrals.referrer_id == user_logs.user_id,
                    "left"
                )
                return merged_df.select(tabletarget["*"], user_logs["timezone_homeclub"])
            return tabletarget

    def adjust_addCol(dataframe, table_name):
        user_referrals = dataframes['user_referrals'].alias("user_referrals")
        lead_logs = dataframes['lead_logs'].alias("lead_logs")
        user_referrals.createOrReplaceTempView("user_referrals")
        lead_logs.createOrReplaceTempView("lead_logs")
        query = """
        SELECT user_referrals.*,
               CASE
                   WHEN user_referrals.referral_source = 'User Sign Up' THEN 'Online'
                   WHEN user_referrals.referral_source = 'Draft Transaction' THEN 'Offline'
                   WHEN user_referrals.referral_source = 'Lead' THEN lead_logs.source_category
                   ELSE 'Unknown'
               END AS referral_source_category
        FROM user_referrals
        LEFT JOIN lead_logs ON user_referrals.referee_id = lead_logs.lead_id
        """
        updated_user_referrals = spark.sql(query)
        return updated_user_referrals

    def adjust_initcap(dataframe, table_name):
        for column in dataframe.columns:
            if isinstance(dataframe.schema[column].dataType, StringType):
                dataframe = dataframe.withColumn(column, initcap(col(column)))
        return dataframe

    def adjust_timestamp_to_local(dataframe, table_name):
        for column in dataframe.columns:
            if isinstance(dataframe.schema[column].dataType, TimestampType):
                dataframe = checkTimezone(table_name, dataframe)
                dataframe = dataframe.withColumn(
                    column,
                    F.from_utc_timestamp(F.col(column), F.col("timezone_homeclub"))
                )
        return dataframe
    
    def missingValueCheck(df):
        for column in df.columns:
            df = df.withColumn(column, 
                F.when((F.col(column) == "") | (F.col(column) == "null"), None).otherwise(F.col(column))
            )
            df = df.withColumn(column, F.when(F.col(column) == "", None).otherwise(F.col(column)))
            df = df.withColumn(column, F.when(F.isnull(F.col(column)), None).otherwise(F.col(column)))
            df = df.withColumn(column, F.trim(F.col(column)))
        df = df.dropna(how="any")
        return df
    
    dataframes_adjusted = {}
    for key, dataframe in dataframes.items():
        if any(isinstance(dataframe.schema[column].dataType, TimestampType) for column in dataframe.columns):
            #mengubah semua data timestamp ke dalam homelocal timezone
            dataframe = adjust_timestamp_to_local(dataframe, key) 
        if any(isinstance(dataframe.schema[column].dataType, StringType) for column in dataframe.columns):            
            #mengubah semua data string init words into Capital
            dataframe = adjust_initcap(dataframe, key)
        if key == 'user_referrals':
            #menambahkan kolom logic untuk user_referrals
            dataframe = adjust_addCol(dataframe, key)
        dataframe = dedupChecking(dataframes, key) # untuk deduplicate data 
        #dataframe = missingValueCheck(dataframe) # untuk data cleansing null values
        dataframes_adjusted[key] = dataframe
    return dataframes_adjusted

def apply_business_logic(dataframes: dict) -> dict:
    """
    Apply business logic to detect potential fraud with add 'is_business_logic_valid' column.
    """
    
    for table_name, df in dataframes.items():
        df.createOrReplaceTempView(table_name)

    query = """
        WITH valid_referrals AS (
            SELECT DISTINCT
                ur_log.id AS referral_details_id,
                ur.referral_id,
                ur.referral_source,
                ll.source_category AS referral_source_category,
                ur.referral_at,
                ur.referrer_id,
                ul.name AS referrer_name,
                ul.phone_number AS referrer_phone_number,
                ul.homeclub AS referrer_homeclub,
                ur.referee_id,
                COALESCE(ur.referee_name, ll.lead_id) AS referee_name,
                COALESCE(ur.referee_phone, ll.id) AS referee_phone,
                ur_ref_status.description AS referral_status,
                DATEDIFF(rr.created_at, ur.referral_at) AS num_reward_days,
                pt.transaction_id AS transaction_id,
                pt.transaction_status,
                pt.transaction_at,
                pt.transaction_location,
                pt.transaction_type,
                ur.updated_at,
                rr.created_at AS reward_granted_at,
                rr.reward_value,
                ul.membership_expired_date,
                ul.is_deleted,
                ur_log.is_reward_granted
            FROM
                user_referrals ur
            LEFT JOIN
                referral_rewards rr ON ur.referral_reward_id = rr.id
            LEFT JOIN
                paid_transactions pt ON ur.transaction_id = pt.transaction_id
            LEFT JOIN
                user_logs ul ON ur.referrer_id = ul.user_id
            LEFT JOIN
                user_referral_statuses ur_ref_status ON ur.user_referral_status_id = ur_ref_status.id
            LEFT JOIN
                user_referral_logs ur_log ON ur.referral_id = ur_log.user_referral_id
            LEFT JOIN
                lead_logs ll ON ur.referee_id = ll.lead_id AND ur.referral_source = 'Lead'
        )
        SELECT
            referral_details_id,
            referral_id,
            referral_source,
            referral_source_category,
            referral_at,
            referrer_id,
            referrer_name,
            referrer_phone_number,
            referrer_homeclub,
            referee_id,
            referee_name,
            referee_phone,
            referral_status,
            num_reward_days,
            transaction_id,
            transaction_status,
            transaction_at,
            transaction_location,
            transaction_type,
            updated_at,
            reward_granted_at,
            CASE
                -- condition 1 Valid
                WHEN (
                    reward_value > 0
                    AND referral_status = 'Berhasil'
                    AND transaction_id IS NOT NULL
                    AND transaction_status = 'PAID'
                    AND transaction_type = 'NEW'
                    AND transaction_at >= referral_at
                    AND MONTH(transaction_at) = MONTH(referral_at)
                    AND membership_expired_date >= referral_at
                    AND is_deleted = FALSE
                    AND is_reward_granted = TRUE
                ) THEN TRUE
                -- condition 2 Valid
                WHEN (
                    reward_value is NULL
                    AND referral_status IN ("Tidak Berhasil","Menunggu")
                ) THEN TRUE
                -- condition 1 Invalid
                WHEN (
                    reward_value > 0
                    AND referral_status != 'Berhasil'
                ) THEN FALSE
                -- condition 2 Invalid
                WHEN (
                    reward_value > 0
                    AND transaction_id IS NULL
                ) THEN FALSE
                -- condition 3 Invalid
                WHEN (
                    reward_value IS NULL
                    AND transaction_id IS NOT NULL
                    AND transaction_status = 'PAID'
                    AND transaction_at > referral_at
                ) THEN FALSE
                -- condition 4 Invalid
                WHEN (
                    referral_status = 'Berhasil'
                    AND (reward_value IS NULL OR reward_value = 0)
                ) THEN FALSE
                -- condition 5 Invalid
                WHEN (
                    transaction_at < referral_at
                ) THEN FALSE
                ELSE NULL
            END AS is_business_logic_valid
        FROM valid_referrals;
        """

    final_df = spark.sql(query)    
    return  final_df


def save_dffinal_to_csv(dataframe: DataFrame, output_file_name: str):
    """
    Save final DataFrame into CSV file
    """
    temp_output_path = "temp_output"
    dataframe.coalesce(1).write.csv(temp_output_path, header=True, mode="overwrite")
    output_file = ""
    for file_name in os.listdir(temp_output_path):
        if file_name.endswith(".csv"):
            output_file = file_name
            break
    if output_file:
        shutil.move(os.path.join(temp_output_path, output_file), output_file_name)
    shutil.rmtree(temp_output_path)


dataframes = load_csv(file_paths) #Task 1
cleaned_dataframes = data_cleansing(dataframes) #Task 2
processed_data = process_data(cleaned_dataframes) #Task 3
final_data = apply_business_logic(processed_data) #Task 4
save_dffinal_to_csv(final_data, "/app/output/final_data.csv") #Task 5

# Stop Spark session
spark.stop()
