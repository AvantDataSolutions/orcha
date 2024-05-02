import ast
import html
import importlib
import inspect
import json
import os
import pkgutil
import typing

import markdown


class DocstringToDocs():

    def __init__(self, root_path: str, override_json: str | None):
        if override_json:
            self.override_dict = json.loads(override_json)
        else:
            self.override_dict = {}
        self.ROOT_PATH = root_path


    def path_to_module_name(self, path: str, root_path: str | None = None):
        root_path = root_path or self.ROOT_PATH
        # remove any leading / from the path
        if path[0] == '/':
            path = path[1:]
        return path[len(root_path):].replace('/', '.').replace('.py', '').replace('.__init__', '')


    def generate_html_for_class_or_function(self, full_name, name, signature, doc):
        md = self.docstring_to_html(doc)
        return f'''
            <p id="{full_name}" style="color: blue;">
                <b>{html.escape(name)}</b>:
                {html.escape(str(signature))}
                <span style="color: green;">{md}</span>
            </p>
        '''

    def docstring_to_html(self, docstring):
        docstring = '\n'.join([line.strip() for line in docstring.split('\n')])
        return markdown.markdown(docstring)

    def get_imports(self, module):
        with open(module.__file__, 'r') as f:
            tree = ast.parse(f.read())
            imports = []
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        imports.append(alias.name)
                elif isinstance(node, ast.ImportFrom):
                    for alias in node.names:
                        if not node.module:
                            continue
                        imports.append(node.module + '.' + alias.name)
            return imports

    def class_needs_processing(self, module, class_):
        if class_[1].__module__ != module.__name__:
            return False
        if '__' in class_[0] or '_' == class_[0][0]:
            return False
        return True

    def format_class(self, module, class_):
        full_name = f'{class_[1].__module__}.{class_[0]}'
        cur_doc_string = class_[1].__doc__
        if full_name in self.override_dict:
            cur_doc_string = self.override_dict[full_name]
        class_sig_str = str(inspect.signature(class_[1]))
        class_extends: list = [f'{b.__module__}.{b.__name__}' for b in class_[1].__bases__]
        class_extends_html = ''
        for b in class_extends:
            if b == 'builtins.object':
                continue
            if 'orcha' in b:
                class_extends_html += f'<a href="#{b}">{b}</a>, '
            else:
                class_extends_html += f'{b}, '
        class_extends_html = class_extends_html[:-2]
        if cur_doc_string is None or cur_doc_string == class_sig_str:
            cur_doc_string = 'No documenation provided'
        doc_string_html = self.docstring_to_html(cur_doc_string)
        return f'''
            <h4 id="{full_name}">{html.escape(class_[0])}{class_sig_str}</h4>
            {f"<h5>Extends: {class_extends_html}</h5>" if class_extends_html else ""}
            <p style="color: blue;">{doc_string_html}</p>
        '''

    def method_needs_processing(self, module, method):
        if method[1].__module__ != module.__name__:
            return False
        if '__' in method[0] or '_' == method[0][0]:
            return False
        return True

    def format_method(self, module, class_name, method):
            full_name = f'{method[1].__module__}.{class_name[0]}.{method[0]}'
            doc = method[1].__doc__
            if full_name in self.override_dict:
                doc = self.override_dict[full_name]
            cur_doc_string = doc if doc is not None else 'No documenation provided'
            return self.generate_html_for_class_or_function(
                name=method[0],
                full_name=full_name,
                signature=inspect.signature(method[1]),
                doc=cur_doc_string
            )

    def function_needs_processing(self, module, function):
        if function[1].__module__ != module.__name__:
            return False
        if '__' in function[0] or '_' == function[0][0]:
            return False
        return True

    def format_function(self, module, function):
        full_name = f'{function[1].__module__}.{function[0]}'
        cur_doc_string = function[1].__doc__ if function[1].__doc__ is not None else 'No documenation provided'
        if full_name in self.override_dict:
            cur_doc_string = self.override_dict[full_name]
        return self.generate_html_for_class_or_function(
            name=function[0],
            full_name=full_name,
            signature=inspect.signature(function[1]),
            doc=cur_doc_string
        )

    def variable_needs_processing(self, module, variable):
        if '__' in variable[0] or '_' == variable[0][0]:
            return False
        return True

    def format_variable(self, module, variable):
        full_name = f'{module.__name__}.{variable[0]}'
        cur_doc_string = variable[1].__doc__ if variable[1].__doc__ is not None else 'No documenation provided'
        if full_name in self.override_dict:
            cur_doc_string = self.override_dict[full_name]
        md = self.docstring_to_html(cur_doc_string)
        return f'''
            <p style="color: blue;" id="{full_name}">
                <b>{html.escape(variable[0])}</b>:
                <span style="color: green;">{md}</span>
            </p>
        '''

    def get_all_modules(self):
        all_modules = []
        def _process(package):
            if isinstance(package, str):
                package = importlib.import_module(package)

            results = {}
            for loader, name, is_pkg in pkgutil.walk_packages(package.__path__):
                full_name = package.__name__ + '.' + name
                all_modules.append(full_name)
                results[full_name] = importlib.import_module(full_name)
                if is_pkg:
                    _process(full_name)

        all_modules.sort()

        _process('orcha')
        return all_modules


    def toc_data_to_html(self, toc_index, toc_items, skip_first_level=True):
        toc_html = ''
        for k, v in toc_index.items():
            if not skip_first_level:
                toc_html += f'<li><a href="#{k}">{k}</a></li>'
                if(len(toc_items.get(k, [])) > 0):
                    toc_html += '<ul>'
                    for item in toc_items.get(k, []):
                        toc_html += f'<li><a href="#{k}.{item}">{item}</a></li>'
                    toc_html += '</ul>'
            if v:
                toc_html += '<ul>'
                for k2, v2 in v.items():
                    toc_html += f'<li><a href="#{k}.{k2}">{k2}</a></li>'
                    if(len(toc_items.get(f'{k}.{k2}', [])) > 0):
                        toc_html += '<ul>'
                        for item in toc_items.get(f'{k}.{k2}', []):
                            toc_html += f'<li><a href="#{k}.{k2}.{item}">{item}</a></li>'
                        toc_html += '</ul>'
                    if v2:
                        toc_html += '<ul>'
                        for k3, v3 in v2.items():
                            toc_html += f'<li><a href="#{k}.{k2}.{k3}">{k3}</a></li>'
                            if len(toc_items.get(f'{k}.{k2}.{k3}', [])) > 0:
                                toc_html += '<ul>'
                                for item in toc_items.get(f'{k}.{k2}.{k3}', []):
                                    toc_html += f'<li><a href="#{k}.{k2}.{k3}.{item}">{item}</a></li>'
                                toc_html += '</ul>'
                            if v3:
                                toc_html += '<ul>'
                                for k4, v4 in v3.items():
                                    toc_html += f'<li><a href="#{k}.{k2}.{k3}.{k4}">{k4}</a></li>'
                                    if len(toc_items.get(f'{k}.{k2}.{k3}.{k4}', [])) > 0:
                                        toc_html += '<ul>'
                                        for item in toc_items.get(f'{k}.{k2}.{k3}.{k4}', []):
                                            toc_html += f'<li><a href="#{k}.{k2}.{k3}.{k4}.{item}">{item}</a></li>'
                                        toc_html += '</ul>'
                                    if v4:
                                        toc_html += '<ul>'
                                        for k5, v5 in v4.items():
                                            toc_html += f'<li><a href="#{k}.{k2}.{k3}.{k4}.{k5}">{k5}</a></li>'
                                            if(len(toc_items.get(f'{k}.{k2}.{k3}.{k4}.{k5}', [])) > 0):
                                                toc_html += '<ul>'
                                                for item in toc_items.get(f'{k}.{k2}.{k3}.{k4}.{k5}', []):
                                                    toc_html += f'<li><a href="#{k}.{k2}.{k3}.{k4}.{k5}.{item}">{item}</a></li>'
                                                toc_html += '</ul>'
                                toc_html += '</ul>'
                        toc_html += '</ul>'
                toc_html += '</ul>'

        if skip_first_level:
            return toc_html
        else:
            return f'<ul>{toc_html}</ul>'


    def generate_docs(self):

        docstring = '<html><body>'
        all_modules = self.get_all_modules()
        toc_index: dict = {}
        toc_items: dict[str, list[str]] = {}


        def _populate_toc(module_name, item_name):

            def _do(cur_toc_level, items_left):
                if items_left[0] not in cur_toc_level:
                    cur_toc_level[items_left[0]] = {}
                if len(items_left) == 1:
                    return
                _do(cur_toc_level[items_left[0]], items_left[1:])

            p = module_name.split('.')
            _do(toc_index, p)

            if module_name not in toc_items:
                toc_items[module_name] = []
            toc_items[module_name].append(item_name)


        def process_module(module):
            nonlocal docstring

            try:
                if '/orcha/' not in str(module.__file__):
                    return
            except AttributeError:
                return

            if 'orcha.docs' in module.__name__ or 'orcha.tests' in module.__name__:
                return

            imports = self.get_imports(module)
            module_full_name = html.escape(self.path_to_module_name(module.__file__))
            docstring += f'<h1 id="{module_full_name}">{module_full_name}</h1>'

            docstring += '<h3>Classes</h3>'
            classes = inspect.getmembers(module, inspect.isclass)
            for c in classes:
                if not self.class_needs_processing(module, c):
                    continue
                _populate_toc(module_name=module.__name__, item_name=c[0])
                class_str = self.format_class(module, c)
                docstring += f'<div style="margin-left: 20px;">{class_str}</div>'

                methods = inspect.getmembers(c[1], inspect.isfunction)
                for m in methods:
                    if not self.method_needs_processing(module, m):
                        continue
                    _populate_toc(module_name=module.__name__, item_name=f'{c[0]}.{m[0]}')
                    method_str = self.format_method(
                        module=module,
                        class_name=c,
                        method=m
                    )
                    docstring += f'<div style="margin-left: 40px;">{method_str}</div>'

            docstring += '<h3>Functions</h3>'
            functions = inspect.getmembers(module, inspect.isfunction)
            for f in functions:
                if not self.function_needs_processing(module, f):
                    continue
                _populate_toc(module_name=module.__name__, item_name=f[0])
                func_str = self.format_function(module, f)
                docstring += f'<div style="margin-left: 20px;">{func_str}</div>'

            variables = inspect.getmembers(
                module,
                lambda x: not(
                    inspect.isfunction(x) or
                    inspect.isclass(x) or
                    inspect.isbuiltin(x) or
                    inspect.ismodule(x) or
                    inspect.isgenerator(x) or
                    isinstance(x, typing.Type)
                )
            )
            variables = [
                v for v in variables
                if str(v[1]) not in imports and
                v[0] != 'annotations' and
                '__' not in v[0] and
                '_' != v[0][0] and
                '~' not in str(v[1])
            ]
            docstring += '<h3>Variables</h3>'
            for v in variables:
                if not self.variable_needs_processing(module, v):
                    continue
                _populate_toc(module_name=module.__name__, item_name=v[0])
                var_str = self.format_variable(module, v)
                docstring += f'<div style="margin-left: 20px;">{var_str}</div>'

        for module in all_modules:
            process_module(importlib.import_module(module))

        toc_html = '<h1>Table of Contents</h1>'
        toc_html += self.toc_data_to_html(toc_index, toc_items)

        doc_header_html = '''
            <div style="display: flex; flex-direction: column; align-items: center; font-size: larger;">
                <img src="https://github.com/AvantDataSolutions/orcha_ui/blob/f08cec698e6a1a97e0e80dca6071a5a7ada65b08/assets/orcha-logo-round.png?raw=true" alt="Orcha Logo" style="width: 100px; height: 100px;">
                <h1>Orcha Documentation</h1>
                <span>This is the documentation for the Orcha ETL framework.</span>
                <span style="width: 90%; text-align: center; padding-top: 15px;">
                    A lightweight ETL framework that focuses on balancing code simplicity for
                    fast development and providing a readable structure to improve debugging
                    and maintainability. Orcha is designed to be easy to
                    use for small and mid-sized ETL projects that value speed of
                    development and improved maintainability.
                </span>
            </div>
        '''


        # import_submodules('orcha')
        docstring += '</body></html>'

        full_html = ''
        # toc_html and docstring in two columns next to each other
        full_html += f'''
            <html>
                <head>
                    <style>
                        .column {{
                            float: left;
                            width: 450px;
                            height: 100vh;
                            overflow: auto;
                            margin-right: 20px;
                        }}
                        .column-auto {{
                            float: left;
                            width: calc(100% - 470px);
                            height: 100vh;
                            overflow: auto;
                        }}
                    </style>
                </head>
                <body>
                    <div class="column" id="top">
                        {toc_html}
                    </div>
                    <div class="column-auto">
                        {doc_header_html}
                        {docstring}
                    </div>
                </body>
            </html>
        '''
        return full_html

# Note: This is designed to be run from the root of the project
# and will generate the docs.html file that can be moved to the
# docs folder when ready to be published.

generator = DocstringToDocs(
    root_path=os.getcwd(),
    override_json='''{
    "orcha.core.tasks.RunType": "The types of runs that can be created.\\n- scheduled: A run that is created by the scheduler\\n- manual: A run that is created manually as a 'one-off'\\n- retry: A run that is created as a retry of a failed run",
    "orcha.core.scheduler.orcha_log": "The logger instanced used by the scheduler",
    "orcha.core.task_runner.BASE_THREAD_GROUP": "The base thread group name for all task threads. Defaults to 'base_thread'.",
    "orcha.utils.sqlalchemy.CHUNK_SIZE": "Sized used for splitting queries sent to the database. Defaults to 1000 (due to mssql limit)."
}''')

full_html = generator.generate_docs()

with open('docs.html', 'w') as f:
    f.write(full_html)

print(generator.get_all_modules())
