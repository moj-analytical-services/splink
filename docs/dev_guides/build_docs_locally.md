## Building docs locally

To rapidly build the documentation and immediately see changes you've made you can use the following code:

```
git clone https://github.com/moj-analytical-services/splink_demos.git demos/
mkdir docs/settingseditor
curl https://raw.githubusercontent.com/moj-analytical-services/splink/splink3/splink/files/settings_jsonschema.json --output docs/settingseditor/settings_jsonschema.json
cat docs/settingseditor/settings_jsonschema.json
echo '<iframe  height="1200" width="100%" src="https://www.robinlinacre.com/splink3_settings_editor_temp" >' >  docs/settingseditor/editor.md

source deactivate
rm -rf venv/
python3 -m venv venv
source venv/bin/activate

pip install --upgrade pip && pip install mkdocs==1.3.0 mknotebooks mkdocs-material mkdocs-autorefs mkdocs-include-markdown-plugin mkdocs-material-extensions mkdocs-simple-plugin mkdocstrings mkdocstrings-python mkdocstrings-python-legacy mkdocs-semiliterate
pip install jinja2==3.0.3


mkdocs serve
```

This is much faster than waiting for github actions to run if you're trying to make fiddly changes to formatting etc.

Here's a working `requirements.txt`

```
astunparse==1.6.3
attrs==22.1.0
beautifulsoup4==4.11.1
bleach==5.0.1
click==8.1.3
defusedxml==0.7.1
entrypoints==0.4
fastjsonschema==2.16.1
ghp-import==2.1.0
gitdb==4.0.9
GitPython==3.1.27
griffe==0.22.0
importlib-metadata==4.12.0
importlib-resources==5.9.0
Jinja2==3.0.3
jsonschema==4.9.0
jupyter-client==7.3.4
jupyter-core==4.11.1
jupyterlab-pygments==0.2.2
Markdown==3.4.1
MarkupSafe==2.1.1
mergedeep==1.3.4
mistune==0.8.4
mkdocs==1.3.0
mkdocs-autorefs==0.4.1
mkdocs-include-markdown-plugin==3.6.1
mkdocs-material==8.3.9
mkdocs-material-extensions==1.0.3
mkdocs-semiliterate==0.4.0
mkdocs-simple-plugin==1.1.1
mkdocstrings==0.19.0
mkdocstrings-python==0.7.1
mkdocstrings-python-legacy==0.2.3
mknotebooks==0.7.1
nbclient==0.6.6
nbconvert==6.5.0
nbformat==5.4.0
nest-asyncio==1.5.5
packaging==21.3
pandocfilters==1.5.0
pkgutil_resolve_name==1.3.10
Pygments==2.12.0
pymdown-extensions==9.5
pyparsing==3.0.9
pyrsistent==0.18.1
python-dateutil==2.8.2
pytkdocs==0.16.1
PyYAML==6.0
pyyaml_env_tag==0.1
pyzmq==23.2.0
six==1.16.0
smmap==5.0.0
soupsieve==2.3.2.post1
tinycss2==1.1.1
tornado==6.2
traitlets==5.3.0
watchdog==2.1.9
webencodings==0.5.1
zipp==3.8.1
```
