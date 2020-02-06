Remove-Item -ErrorAction SilentlyContinue ./dist/*
Remove-Item -ErrorAction SilentlyContinue -Recurse -Confirm:$false ./*.egg-info
python setup.py sdist bdist_wheel
twine upload dist/*
