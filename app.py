import boto3
import pymysql
import pandas as pd
from io import StringIO

# Set your database connection parameters
db_host = os.environ.get('db_host')
db_user = os.environ.get('db_user')
db_password = os.environ.get('db_password')
db_name = os.environ.get('db_name')

aws_access_key_id =  os.environ.get('aws_access_key_id')
aws_secret_access_key =  os.environ.get('aws_secret_access_key')
s3_bucket =  os.environ.get('s3_bucket')
s3_key =  os.environ.get('s3_key')

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
        COALESCE((select SUM(lad.value)
                    from leave_applications la
                            inner join leave_application_days lad on la.id = lad.leave_application_id
                            inner join leave_types lt on la.leave_type_id = lt.id
                            inner join leave_calenders lc on la.leave_calender_id = lc.id
                    where la.deleted_at is null
                    and lad.deleted_at is null
                    and la.status != 'Rejected'
                    and lt.is_earned = 1
                    and la.employee_id = e.id
                    and lc.status = 1
                    group by la.leave_type_id), 0)                      as days_taken,
        (select coalesce(sum(lb.current_balance + lb.balance_brought_forward), 0)
        from leave_balances lb
                    inner join leave_types lt on lb.leave_types_id = lt.id
                    inner join leave_calenders lc on lb.leave_calender_id = lc.id
        where lb.deleted_at is null
            and lt.is_earned = 1
            and lc.status = 1
            and lb.employee_id = e.id)                                 as opening_balance,
        (select sum(la.value)
        from leave_assignments la
                    inner join leave_types lt on la.leave_type_id = lt.id
                    inner join leave_calenders lc on la.leave_calender_id = lc.id
        where la.deleted_at is null
            and lt.is_earned = 1
            and lc.status = 1
            and la.employee_id = e.id)                                 as accruals,
        COALESCE((SELECT opening_balance + accruals - days_taken), 0) AS leave_balance,
        COALESCE((SELECT leave_balance * ci.salary/26), 0)               AS leave_liability
    from employees e
            left join contract_issuances ci on ci.employee_id = e.id
            inner join departments d on e.department_id = d.id
            inner join designations ds on ds.id = e.designation_id
    where e.deleted_at is null
    and ci.deleted_at is null
    and ci.is_terminated = 0
    and (ci.contract_end_date is null or ci.contract_end_date > now());
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


