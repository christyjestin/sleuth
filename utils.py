import re, string

__all__ = ['strip_punc']

punc_regex = re.compile(f'[{re.escape(string.punctuation)}]')
def strip_punc(text: str):
    return punc_regex.sub('', text)