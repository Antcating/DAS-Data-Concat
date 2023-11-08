from concat.main import main

try:
    main()
except Exception as err:
    print(err.args, err.__cause__)
