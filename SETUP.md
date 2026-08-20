# Release Guide

How to build `general_utils` and publish it to PyPI.

The package is live at <https://pypi.org/project/general-utils/>. PyPI normalises
the name to `general-utils`; both `pip install general_utils` and
`pip install general-utils` resolve to it.

---

## 1. One-time setup

### Tooling

```bash
python -m pip install --upgrade setuptools wheel twine
```

### PyPI account and token

1. Create accounts on [PyPI](https://pypi.org/account/register/) and
   [TestPyPI](https://test.pypi.org/account/register/).
2. Enable 2FA (mandatory on PyPI for maintainers).
3. Create an API token: **Account settings → API tokens → Add API token**.
   Scope it to the `general-utils` project once it exists.
4. Save it in `~/.pypirc`:

```ini
[distutils]
index-servers =
    pypi
    testpypi

[pypi]
username = __token__
password = pypi-AgEIcHlwaS5vcmc...        # your real token

[testpypi]
repository = https://test.pypi.org/legacy/
username = __token__
password = pypi-AgENdGVzdC5weXBpLm9yZw... # separate TestPyPI token
```

```bash
chmod 600 ~/.pypirc
```

`username` is the literal string `__token__` — not your account name.
Never commit `~/.pypirc`.

---

## 2. Pre-release checklist

- [ ] Working tree is clean and on `master` (`git status`)
- [ ] **Bump `version=` in [setup.py](setup.py)** — PyPI permanently rejects a
      re-upload of an existing version, even after deletion
- [ ] `requirements.txt` reflects any new imports
- [ ] README renders correctly (it becomes the PyPI project page)
- [ ] **No secrets in the code** — DSNs, tokens, passwords. Everything published
      to PyPI is public and permanent:
      ```bash
      grep -rnE "dsn=|password|secret|token" general_utils/*.py
      ```
- [ ] `.env` is not tracked (`git ls-files general_utils/.env` returns nothing)

### Versioning

`MAJOR.MINOR.PATCH`. This project has been incrementing the patch digit
(`0.1.16` → `0.1.17`). Bump the minor when the public API changes shape.

---

## 3. Build

Remove stale artefacts first, or old versions get re-uploaded:

```bash
rm -rf build/ dist/ *.egg-info/
python setup.py sdist bdist_wheel
```

This produces two files in `dist/`:

| File | Type | Purpose |
| --- | --- | --- |
| `general_utils-X.Y.Z-py3-none-any.whl` | wheel | what `pip install` normally uses |
| `general_utils-X.Y.Z.tar.gz` | sdist | source fallback |

> **Note:** `python -m build` — the modern replacement — currently **fails** on
> this project. See [Known issues](#known-issues) below. Use the `setup.py`
> command above until `setup.py` is fixed; it emits deprecation warnings but
> works.

---

## 4. Validate before uploading

```bash
twine check dist/*
```

Both files must report `PASSED`. This catches a malformed README, which PyPI
rejects *after* accepting the version number — burning that version permanently.

Inspect what you are about to ship:

```bash
tar tzf dist/general_utils-*.tar.gz
unzip -l dist/general_utils-*.whl
```

Confirm no `.env`, no credentials, no stray local files.

---

## 5. Upload to TestPyPI first

```bash
twine upload --repository testpypi dist/*
```

Then install it in a throwaway environment:

```bash
python -m venv /tmp/verify && source /tmp/verify/bin/activate
pip install --index-url https://test.pypi.org/simple/ \
            --extra-index-url https://pypi.org/simple/ general_utils
python -c "from general_utils import utils; print(utils.__doc__)"
deactivate
```

`--extra-index-url` is required because TestPyPI does not mirror the real
dependencies (`pika`, `minio`, `redis`, …).

---

## 6. Upload to PyPI

```bash
twine upload dist/*
```

Verify:

```bash
pip install --upgrade general_utils
python -c "import general_utils, general_utils.utils as u; print(u.__doc__)"
```

The release appears at <https://pypi.org/project/general-utils/>.

---

## 7. Tag the release

```bash
git tag -a v0.1.17 -m "Release 0.1.17"
git push origin v0.1.17
```

Tagging is what lets you reconstruct exactly what went into a given release —
the `dist/` folder is git-ignored on purpose.

---

## Known issues

These are pre-existing problems in [setup.py](setup.py), documented so the build
steps above make sense. None of them block a release using the legacy command.

### `python -m build` fails

[setup.py](setup.py) reads its dependencies through a **private pip API**:

```python
from pip._internal.req import parse_requirements
```

`python -m build` runs in an isolated environment that contains setuptools and
wheel but **not pip**, so the import raises `ModuleNotFoundError: No module
named 'pip'`. Being a private API, it also breaks across pip releases without
warning — `ir.requirement` was `ir.req` before pip 20.1.

### `requirements.txt` is missing from the sdist

There is no `MANIFEST.in`, so `general_utils/requirements.txt` is not included
in the source distribution — confirmed absent from the published 0.1.16 sdist.
Building from the sdist therefore fails:

```
InstallationError: Could not open requirements file:
[Errno 2] No such file or directory: '.../general_utils/requirements.txt'
```

Normal `pip install general_utils` is unaffected, because it takes the wheel,
which has its dependencies baked into the metadata at build time. Only
source-based installs (`pip install --no-binary :all:`) break.

### Recommended fix

Parsing the file directly removes both problems and makes `python -m build`
work:

```python
requirements_path = os.path.join(dirname, "general_utils/requirements.txt")
with open(requirements_path) as f:
    reqs = [line.strip() for line in f
            if line.strip() and not line.startswith("#")]
```

Then add a `MANIFEST.in` so the sdist carries the file:

```
include general_utils/requirements.txt
include README.md
```

### Missing metadata

[setup.py](setup.py) declares GPLv3 in its classifiers, but there is no
`LICENSE` file in the repository and no `license=` field, so PyPI shows the
license as unset. `python_requires=` is likewise unset, so pip will offer the
package to Python versions it was never tested on.
