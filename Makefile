build:
# Build the package
	@python -m build

clean:
# Clean the build directory
	@rm -rf dist

bump_patch:
# Increase the patch number
	@python version_bump.py

bump_minor:
# Increase the minor number
	@python version_bump.py minor

bump_major:
# Increase the major number
	@python version_bump.py major

upload: clean build
# Upload the package to PyPI
	@python -m twine upload dist/*