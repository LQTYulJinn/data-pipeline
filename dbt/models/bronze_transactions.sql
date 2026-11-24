select * from read_parquet('{{ env_var("BRONZE_GLOB", "output/local/bronze/*.parquet") }}')
