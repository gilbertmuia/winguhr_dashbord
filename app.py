import boto3
import pymysql
import pandas as pd
from io import StringIO
from dotenv import load_dotenv
import os

load_dotenv()

# Set your database connection parameters
db_host = os.getenv('db_host')
db_user = os.getenv('db_user')
db_password = os.getenv('db_password')
db_name = os.getenv('db_name')

aws_access_key_id =  os.getenv('aws_access_key_id')
aws_secret_access_key =  os.getenv('aws_secret_access_key')
s3_bucket =  os.getenv('s3_bucket')
s3_key =  os.getenv('s3_key')

companies = [
    'winguhr_10_supaflo',
    'winguhr_11_abm',
    'winguhr_12_kikoymall',
    'winguhr_13_pfnl',
    'winguhr_15_pgpmoz',
    'winguhr_16_kaf',
    'winguhr_16_olympian',
    'winguhr_17_mininrb',
    'winguhr_18_pgp',
    'winguhr_18_towertech',
    'winguhr_19_matola',
    'winguhr_19_minimsa',
    'winguhr_1_11degrees',
    'winguhr_20_oakhouse',
    'winguhr_21_cosmocare',
    'winguhr_21_demo',
    'winguhr_21_olympian',
    'winguhr_22_rhea',
    'winguhr_23_hananasteel',
    'winguhr_23_hsl',
    'winguhr_24_test',
    'winguhr_25_tests',
    'winguhr_2_jisaidie',
    'winguhr_3_art',
    'winguhr_5_whitespace',
    'winguhr_6_trident',
    'winguhr_8_miniholdings',
    'winguhr_9_aexcel'                                                                                                                        
]

all_results_df = pd.DataFrame()

for company in companies:
    sql_query = """

    select 
        '""" + company + """'
        AS company,
        e.id,
        e.first_name,
        e.middle_names,
        e.last_name,
        d.name                                                        as department_name,
        ds.name                                                       as designation,
        ci.salary,
        lt.name as leave_type,
            COALESCE((select SUM(lad.value)
                        from leave_applications la
                                inner join leave_application_days lad on la.id = lad.leave_application_id
                                inner join leave_types lt on la.leave_type_id = lt.id
                        where la.deleted_at is null
                        and lad.deleted_at is null
                        and la.status != 'Rejected'
                        and lt.id = lb.leave_types_id
                        and la.employee_id = e.id
                        group by la.leave_type_id), 0) as days_taken,

            COALESCE((
                select sum(lbs.current_balance + lbs.balance_brought_forward)
                from leave_balances lbs
                            inner join leave_types lt on lbs.leave_types_id = lt.id
                where lbs.deleted_at is null
                    and lt.id = lbs.leave_types_id
                    and lbs.employee_id = e.id
                    and lbs.id = lb.id
            ),0) as opening_balance,
            COALESCE((
                select sum(la.value)
                from leave_assignments la
                            inner join leave_types lt on la.leave_type_id = lt.id
                where la.deleted_at is null
                    and lt.id = lb.leave_types_id
                    and la.employee_id = e.id
                ),0) as accruals,
            COALESCE((SELECT opening_balance + accruals - days_taken), 0) AS leave_balance,
            COALESCE(
            (
                SELECT IF(lt.is_earned = 1,leave_balance * ci.salary/26,0)
            ), 0) 
            AS leave_liability
        from leave_balances lb
                inner join employees e on e.id = lb.employee_id
                inner join contract_issuances ci on ci.employee_id = e.id
                inner join departments d on e.department_id = d.id
                inner join designations ds on ds.id = e.designation_id
                inner join leave_types lt on lt.id = lb.leave_types_id
                inner join leave_calenders lc on lc.id = lb.leave_calender_id
        where e.deleted_at is null
        and ci.deleted_at is null
        and ci.is_terminated = 0
        and (ci.contract_end_date is null or ci.contract_end_date > now())
        and lc.status = 1
    """
    
    # print(sql_query)


    try:
        # Connect to the database
        conn = pymysql.connect(host=db_host, user=db_user, password=db_password, database=company)
        cursor = conn.cursor()

        # Execute the SQL query
        cursor.execute(sql_query)

        # Fetch the results
        results = cursor.fetchall()

        print(f"Query returned {len(results)} rows for company: {company}")

        # Close the database connection
        conn.close()
        
        # Convert the results to a pandas DataFrame for each iteration
        columns = [desc[0] for desc in cursor.description]
        iteration_df = pd.DataFrame(results, columns=columns)
        
        # check if the DataFrame is empty
        if iteration_df.empty:
            print(f"Query returned no rows for company: {company}")
            continue
        
        
        

        # Append the DataFrame of the current iteration to the main DataFrame
        all_results_df = all_results_df._append(iteration_df, ignore_index=True)
    except TypeError:
        print(f"Query returned no rows for company: {company}")

# Save DataFrame to CSV in-memory
csv_buffer = StringIO()
all_results_df.to_csv(csv_buffer, index=False)
    
    
    
    
    

# Connect to S3
s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

# Upload the CSV file to S3
s3_client.put_object(Body=csv_buffer.getvalue(), Bucket=s3_bucket, Key=s3_key)

print(f"Data saved to S3 bucket: {s3_bucket}/{s3_key}")


