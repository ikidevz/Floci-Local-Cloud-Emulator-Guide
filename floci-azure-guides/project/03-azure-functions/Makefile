# Function zip = function_app.py + host.json + vendored dependencies (the runtime container has NO internet-installed packages).
# The runtime image is Linux x86-64 Debian 12. On an ARM host:  make package PLATFORMS="--platform manylinux_2_28_aarch64 --platform manylinux2014_aarch64"
PLATFORMS ?= --platform manylinux_2_28_x86_64 --platform manylinux2014_x86_64
PY ?= python

package:
	rm -rf build && mkdir -p build/app/.python_packages/lib/site-packages
	$(PY) -m pip install -r requirements.txt -t build/app/.python_packages/lib/site-packages $(PLATFORMS) --only-binary=:all: --python-version 3.12 --implementation cp
	cp function_app.py host.json build/app/
	cd build/app && $(PY) -m zipfile -c ../demo_fn.zip . && cd ../.. && ls -l build/demo_fn.zip
