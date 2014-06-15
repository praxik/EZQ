require 'yaml'

input_file = ARGV[0]

(1..5).to_a.each do |idx|
  pgcf = {}
  pgcf['command'] = "6k_pregrid.exe -c ODBC --leafconnstr Server=development-rds-pgsq.csr7bxits1yb.us-east-1.rds.amazonaws.com;Port=5432;Uid=app;Pwd=app;Database=praxik; --ssurgoconnstr Server=10.1.2.8;Port=5432;Uid=postgres;Pwd=postgres;Database=ssurgo; -g iowammp.gdb -j $jobid -t isa_run1_scn -m #{idx} -x #{idx}"
  File.write('pre_grid_command.yml',pgcf.to_yaml)
  system("ruby job_breaker.rb --log job_breaker.log -c 6k_job_breaker_config.yml -e \"ruby Pre_grid_wrapper.rb #{input_file}\"")
end
