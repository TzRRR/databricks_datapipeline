install:
	pip install --upgrade pip &&\
		pip install -r requirements.txt

query:
	python main.py

test:
	python -m pytest -vv test_main.py mylib/test_etl.py

format:	
	black *.py 

lint:
	#disable comment to test speed
	#pylint --disable=R,C --ignore-patterns=test_.*?py *.py mylib/*.py
	#ruff linting is 10-100X faster than pylint
	ruff check *.py mylib/*.py

refactor: format lint
		
all: install lint test format deploy
