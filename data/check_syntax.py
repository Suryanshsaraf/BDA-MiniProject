with open('react-dashboard/index.html', encoding='utf-8') as f:
    content = f.read()

start_marker = '<script type="text/babel">'
end_marker = '</script>'
start = content.find(start_marker) + len(start_marker)
end = content.rfind(end_marker)
js = content[start:end]

for o, c in [('(', ')'), ('{', '}'), ('[', ']')]:
    diff = js.count(o) - js.count(c)
    print(f"{o}{c}: diff={diff} (open={js.count(o)}, close={js.count(c)})")
