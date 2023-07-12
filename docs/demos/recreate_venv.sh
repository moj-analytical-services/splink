source deactivate
rm -rf venv/
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip

pip install -r requirements.txt
python -m ipykernel install --user --name=splink_demos