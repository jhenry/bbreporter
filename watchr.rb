watch('.*\.py') { |md| system("py.test -rx test_bbreporting.py") }
