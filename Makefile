build:
	python -m build

push:
	python -m twine upload --repository local dist/*
