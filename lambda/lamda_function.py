import boto3
import pandas
import time
import timeit

# initialise
s3_client = boto3.client('s3')
glue_client = boto3.client('glue')

def lambda_handler(event, context):
    bucket_name = event['Records'] [0] ['s3'] ['bucket'] ['name']
    key = event['Records'] [0] ['s3'] ['object'] ['key']
    
    df = get_s3_dataset_df(bucket_name, key)
    
    # quality check 01 - check df has data
    if (check_df_empty(df)):
        print(f'Dataset returned empty - please check file, {bucket_name}/{key}')
    else:
        print('Check passed - dataset exists')
        
    # quality check 02 - check null data for key fields. Pressure field added to show the check works
    null_check_fields = ['ObservationDate', 'Region', 'ScreenTemperature', 'Pressure']
    
    for col in null_check_fields:
        check_null(df, col)
    
    # more data quality checks can go here...
        
    # this function triggers all the Glue processing so the csv file is converted to Parquet
    # and is Athena query ready
    trigger_glue_tasks()
    
    return 1
    
def get_s3_dataset_df(bucket_name, key):
    
    # lambda can download to /tmp/ only
    local_file = '/tmp/' + 'weather_data.csv'
    df = pandas.DataFrame()
    
    try:
        resp = s3_client.download_file(bucket_name, key, local_file)
        if resp == None:
            df = pandas.read_csv('/tmp/weather_data.csv')
            print(df)
        return df
    except Exception as e:
        print(e)
        raise e
    
def check_df_empty(df):
    if df.empty:
        return True
    return False
    
def check_null(df,column):
    if (df[column].hasnans):
        print(f'Nulls in {column}, please check')

def trigger_glue_tasks():
    timeout_seconds = 600
    start_time = timeit.default_timer()
    abort_time = start_time + timeout_seconds
    
    # helps handle timeout state
    def check_for_timeout() -> None:
        if timeit.default_timer() > abort_time:
            raise TimeoutError(f"Failed to crawl {crawler}. The allocated time of {timeout_minutes:,} minutes has elapsed.")
    
    # function to trigger Glue crawlers
    def run_and_check_crawler(crawler):
        state_previous = None
        retry_seconds = 10
        
        glue_client.start_crawler(Name=crawler)
        
        while True:
            response_get = glue_client.get_crawler(Name=crawler)
            state = response_get["Crawler"]["State"]
            if state != state_previous:
                print(f"Crawler {crawler} is {state.lower()}.")
                state_previous = state
            if state == "READY":  # Other known states: RUNNING, STOPPING
                return
            check_for_timeout()
            time.sleep(retry_seconds)
    
    # function to trigger Glue job for Parquet conversion
    def run_and_check_job(job):
        state_previous = None
        retry_seconds = 10
        
        response = glue_client.start_job_run(JobName=job)
        
        while True:
            response_get = glue_client.get_job_run(JobName=job, RunId=response['JobRunId'])
            state = response_get['JobRun']['JobRunState']
            if state != state_previous:
                print(f"Job {job} is {state.lower()}.")
                state_previous = state
            if state == "SUCCEEDED":  # Other known states: 'STOPPED', 'FAILED', 'TIMEOUT'
                return
            check_for_timeout()
            time.sleep(retry_seconds)
    
    run_and_check_crawler(crawler='dlg-test-csv')
    run_and_check_job(job='dlg-test')
    run_and_check_crawler(crawler='dlg-test-parquet')
