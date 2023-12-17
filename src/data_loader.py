import gdown
from pathlib import Path


def upload_dataset():
    filepath = 'file/nyc_yelow_tiny.csv'
    if not Path(filepath).exists():
        Path('file').mkdir(parents=True, exist_ok=True)
        url = 'https://drive.google.com/uc?export=download&id=1XWCk4XmgdNUZ8E42ktjGpeeKZeTO9YnJ'
        gdown.download(url, filepath, quiet=False)