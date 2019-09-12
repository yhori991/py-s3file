dev:
	pipenv install --dev
	pipenv run pip install -e .
dists: requirements sdist bdist wheels
requirements: 
	pipenv run pipenv_to_requirements
sdist: requirements
	pipenv run python setup.py sdist
bdist: requirements
	pipenv run python setup.py bdist
wheels: requirements
	pipenv run python setup.py bdist_wheel
test_upload: requirements
	pipenv run twine upload --repository testpypi dist/*
upload: requirements
	pipenv run twine upload --repository pypi dist/*
