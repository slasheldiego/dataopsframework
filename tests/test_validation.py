import re

def test_email_regex():
    pattern = r'^[^@\s]+@[^@\s]+\.[^@\s]+$'
    assert re.match(pattern, 'ana@example.com')
    assert not re.match(pattern, 'bad@')
