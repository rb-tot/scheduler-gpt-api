from db_queries import job_pool_df, eligibility_df, technicians_df

print("job_pool rows:", len(job_pool_df()))
print("eligibility rows:", len(eligibility_df()))
print("technicians rows:", len(technicians_df()))
