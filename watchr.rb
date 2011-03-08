watch('.*\.py') { |md| system("py.test -vrx test_bbreporting.py") }
