import re

from bs4 import BeautifulSoup
from markdown import markdown

def md_to_text(md):
    links = re.findall(r'\[([^\]]+)\]\(([^)]+)\)', md)
    
    for text, url in links:
        md = md.replace(f'[{text}]({url})', f'{text} {url}')
    
    html = markdown(md)
    soup = BeautifulSoup(html, features='html.parser')
    return soup.get_text()