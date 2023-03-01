## Building docs locally

To rapidly build the documentation and immediately see changes you've made you can use [this script](https://github.com/moj-analytical-services/splink/scripts/make_docs_locally.sh):

```sh
source scripts/make_docs_locally.sh
```

This is much faster than waiting for github actions to run if you're trying to make fiddly changes to formatting etc.

The Splink repo contains a [working `requirements.txt` for building the docs](https://github.com/moj-analytical-services/splink/scripts/docs-requirements.txt), or a more complete version:

```
attrs==22.2.0
beautifulsoup4==4.11.2
bleach==6.0.0
certifi==2022.12.7
charset-normalizer==3.0.1
click==8.1.3
colorama==0.4.6
defusedxml==0.7.1
EditorConfig==0.12.3
fastjsonschema==2.16.2
ghp-import==2.1.0
gitdb==4.0.10
GitPython==3.1.31
griffe==0.25.5
idna==3.4
importlib-metadata==6.0.0
Jinja2==3.0.3
jsbeautifier==1.14.7
jsonschema==4.17.3
jsonschema2md==0.4.0
jupyter_client==8.0.3
jupyter_core==5.2.0
jupyterlab-pygments==0.2.2
Markdown==3.3.7
MarkupSafe==2.1.2
mergedeep==1.3.4
mistune==2.0.5
mkdocs==1.4.2
mkdocs-autorefs==0.4.1
mkdocs-click==0.8.0
mkdocs-gen-files==0.4.0
mkdocs-include-markdown-plugin==4.0.3
mkdocs-material==8.5.11
mkdocs-material-extensions==1.1.1
mkdocs-mermaid2-plugin==0.6.0
mkdocs-monorepo-plugin==1.0.4
mkdocs-schema-reader==0.11.1
mkdocs-semiliterate==0.7.0
mkdocs-simple-plugin==2.1.2
mkdocstrings==0.20.0
mkdocstrings-python==0.8.3
mkdocstrings-python-legacy==0.2.3
mknotebooks==0.7.1
nbclient==0.7.2
nbconvert==7.2.9
nbformat==5.7.3
packaging==23.0
pandocfilters==1.5.0
platformdirs==3.0.0
Pygments==2.14.0
pymdown-extensions==9.9.2
pyrsistent==0.19.3
python-dateutil==2.8.2
python-slugify==8.0.0
pytkdocs==0.16.1
PyYAML==6.0
pyyaml_env_tag==0.1
pyzmq==25.0.0
requests==2.28.2
six==1.16.0
smmap==5.0.0
soupsieve==2.4
text-unidecode==1.3
tinycss2==1.2.1
tornado==6.2
traitlets==5.9.0
urllib3==1.26.14
watchdog==2.2.1
webencodings==0.5.1
zipp==3.14.0
```
