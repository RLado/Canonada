build:
# Increase the patch number
	@python version_bump.py
# Build the package
	@python -m build

clean:
	@rm -rf dist