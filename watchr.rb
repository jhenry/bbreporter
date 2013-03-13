watch('.*\.py') { |md| system("python -m py.test -rx test_report_runner.py") }
