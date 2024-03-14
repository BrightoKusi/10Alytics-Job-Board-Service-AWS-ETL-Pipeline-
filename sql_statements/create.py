transformed_jobs_data = '''CREATE TABLE IF NOT EXISTS raw_data.transformed_jobs_data (
    job_id VARCHAR(65535),
    job_employment_type VARCHAR(65535),
    job_title VARCHAR(65535),
    job_apply_link VARCHAR(65535),
    job_description VARCHAR(65535),
    job_city VARCHAR(65535),
    job_country VARCHAR(65535),
    job_posted_at_timestamp TIMESTAMP,
    employer_website VARCHAR(65535),
    employer_company_type VARCHAR(65535)
);
'''
