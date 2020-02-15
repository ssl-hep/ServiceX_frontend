Remove-Item -ErrorAction SilentlyContinue -Recurse ./dist/*
Remove-Item -ErrorAction SilentlyContinue -Recurse ./build/*
Remove-Item -ErrorAction SilentlyContinue -Recurse -Confirm:$false ./*.egg-info
python setup.py sdist bdist_wheel
twine upload dist/*
Remove-Item -ErrorAction SilentlyContinue -Recurse ./dist/*
Remove-Item -ErrorAction SilentlyContinue -Recurse ./build/*
Remove-Item -ErrorAction SilentlyContinue -Recurse -Confirm:$false ./*.egg-info
