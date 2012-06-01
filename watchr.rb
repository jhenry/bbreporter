watch('.*\.py') { |md| system("py.test -rx test_report_runner.py") }
