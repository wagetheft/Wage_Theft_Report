install:
	@pip install -r requirements/requirements.txt

savePackages: 
	@pip freeze > requirements/requirements.txt

start: 
	@bash bin/run.sh