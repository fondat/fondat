"""Module to expose resources through a command-line interface."""

# Copyright © 2018 Paul Bryan.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import json
import readline
import re
import roax.context as context
import roax.schema as s
import shlex
import sys
import traceback

from io import TextIOWrapper
from roax.resource import ResourceError
from shutil import copyfileobj
from textwrap import dedent


_re = re.compile("(_*)(.*)(_*)")

def _p2a(name):
    m = _re.match(name)
    return m.group(1) + m.group(2).replace("_", "-") + m.group(3)

def _a2p(name):
    m = _re.match(name)
    return m.group(1) + m.group(2).replace("-", "_") + m.group(3)

def _is_binary(schema):
    return schema and isinstance(schema, s.bytes) and schema.format == "binary"

def _parse_arguments(params, args):
    """Parse arguments for supported operation parameters."""
    result = {}
    args = list(args)
    name = None
    while args:
        arg = args.pop(0)
        if name is None:
            if not arg.startswith("--"):
                raise ValueError
            arg = arg[2:]
            name, value = arg.split("=", 1) if "=" in arg else (arg, None)
            name = _a2p(name)
            if name == "_body" or name not in params:
                raise ValueError
            if value:
                result[name] = value
                name = None
        else:
            result[name] = arg
            name = None
    return result

def _parse_redirects(args, body, returns):
    result = {}
    n = 0
    while n < len(args):
        redir = None
        if args[n] in ("<", ">", ">>"):  # redirection as its own argument
            redir = args.pop(n)
        elif args[n].startswith(">>"):
            redir = ">>"
            args[n] = args[n][2:]
        elif args[n].startswith("<") or args[n].startswith(">"):
            redir = args[n][0]
            args[n] = args[n][1:]
        if not redir:
            n += 1
            continue
        try:
            filename = args.pop(n)
        except IndexError:
            raise ValueError("no redirection file name specified")
        if "<" in filename or ">" in filename:
            raise ValueError("invalid redirection file name")
        if redir:
            result[redir] = filename
    return result

def _read(inp, schema):
    if isinstance(inp, TextIOWrapper):
        inp = inp.buffer
    value = inp.read()
    if _is_binary(schema):
        return schema.bin_decode(value)
    elif isinstance(schema, s.dict) or isinstance(schema, s.list):
        return schema.json_decode(value.decode())
    else:
        return schema.str_decode(value.decode())

def _write(out, schema, value):
    if isinstance(out, TextIOWrapper):
        out = out.buffer
    if _is_binary(schema):
        value = schema.bin_encode(value)
    elif isinstance(schema, s.dict) or isinstance(schema, s.list):
        value = json.dumps(schema.json_encode(value)).encode()
    else:
        value = schema.str_encode(value).encode()
    out.write(value)
    out.flush()


class _open_redirects:

    def __init__(self, inp, out, args, body, returns):
        self.redirs = _parse_redirects(args, body, returns)
        self.in_out = [inp, out]
        self.body_returns = [body, returns]

    def __enter__(self):
        modes = {"<": "rb", ">": "wb", ">>": "ab"}
        offset = {"<": 0, ">": 1, ">>": 1}
        for r in list(self.redirs):
            file = open(self.redirs[r], modes[r])
            self.in_out[offset[r]] = file
            self.redirs[r] = file
        return tuple(self.in_out)

    def __exit__(self, *args):
        for file in self.redirs.values():
            try:
                file.close()
            except:
                pass


class CLI:
    """Command line interface that exposes registered resources."""

    def __init__(self, *, name=None, prompt=None, debug=False, err=sys.stderr):
        """
        Initialize a command line interface.

        :param name: The name of the application.
        :param prompt: The prompt to display for each command.
        :param silent: Do not display prompts and status messages.
        :param debug: Display details for raised exceptions.
        :param inp: Input stream for reading bodies. (default: stdin)
        :param out: Output stream for writing responses. (default: stdout)
        :param err: Output stream for writing prompts. (default: stderr)
        """
        super().__init__()
        self.name = name
        self.prompt = prompt or "{}> ".format(name) if name else "> "
        self.debug = debug
        self.err = err
        self.resources = {}
        self.private = set()
        self.commands = {}
        self._looping = False
        self._init_commands()

    def register(self, name, resource, publish=True):
        """
        Register a resource with the command line interface.

        :param name: The name to expose for the resource via command line.
        :param resource: The resource to be registered.
        :param publish: List the resource in help listings.
        """
        self.resources[name] = resource
        if not publish:
            self.private.add(name)

    def loop(self):
        """Repeatedly issue a command prompt and process input."""
        if self._looping:
            raise ValueError("already looping")
        self._looping = True
        while self._looping:
            try:
                self.process(input(self.prompt))
            except (EOFError, KeyboardInterrupt):
                break
        self._looping = False

    def process(self, line, inp=sys.stdin, out=sys.stdout):
        """
        Process a single command line.
        
        :param line: Command line string to process.
        :return: True if command line was processed successfully.
        """
        try:
            args = shlex.split(line)
            if not args:
                return True
            with context.push(context_type="cli", cli_command=line):
                name = args.pop(0)
                if name in self.resources:
                    return self._process_resource(name, args, inp, out)
                elif name in self.commands:
                    return self.commands[name][0](args)
                else:
                    self._print("Invalid command or resource: {}.".format(name))
                    return False
        except (EOFError, KeyboardInterrupt):
            raise
        except Exception as e:
            self._print("ERROR: {}".format(e))
            if self.debug:
                traceback.print_exc()
            return False

    def _init_commands(self):
        self.commands["help"] = (self._help, "Request help with commands and resources.")
        self.commands["exit"] = (self._exit, "Exit the {} command line.".format(self.name))
        self.commands["quit"] = (self._exit, None)  # alias
        self.commands["q"] = (self._exit, None)  # alias

    def _help(self, args):
        """\
        Usage: help [resource [operation] | command]
          Provide help with commands and resources.\
        """
        name = args.pop(0) if args else None
        if not name:
            return self._help_list()
        elif name in self.resources:
            return self._help_resource(name, args)
        elif name in self.commands:
            return self._help_command(name)
        self._print("Unrecognized resource or command: {}.".format(name))
        return False

    def _exit(self, args):
        """\
        Usage: exit
          Exit the command line interface.\
        """
        raise EOFError

    def _print(self, *args):
        if self.err:
            print(*args, file=self.err)

    def _print_listing(self, listing, indent="", space=4, max_column=24):
        """Sort a dictionary by key and print as a listing."""
        names = sorted(listing.keys())
        ljust = 0
        for name in names:
            if len(name) <= max_column and len(name) > ljust:
                ljust = len(name)
        for name in names:
            self._print("{}{}{}{}".format(indent, name.ljust(ljust), " " * space, listing[name]))    

    def _help_command(self, name):
        """Print the function docstring of a command as help text."""
        self._print(dedent(self.commands[name][0].__doc__))
        return False

    def _process_resource(self, resource_name, args, inp, out):
        resource = self.resources[resource_name]
        operation_name = _a2p(args.pop(0)) if args else None
        operation = resource.operations.get(operation_name)
        if not operation:
            return self._help_resource(resource_name)
        params = operation.params or {}
        returns = operation.returns
        body = params.get("_body")
        with _open_redirects(inp, out, args, body, returns) as (inp, out):
            try:
                parsed = _parse_arguments(params, args)
            except ValueError:
                return self._help_operation(resource_name, operation)
            try:
                for name in parsed:
                    parsed[name] = params[name].str_decode(parsed[name])
                for name in (n for n in params if n != "_body"):
                    if params[name].required and name not in parsed:
                        raise s.SchemaError("missing required parameter")
                if body:
                    name = "{body}"
                    description = (body.description or "{}.".format(name)).lower()
                    if inp == sys.stdin:
                        self._print("Enter {}".format(description))
                        self._print("When complete, input EOF (*nix: Ctrl-D, Windows: Ctrl-Z+Return):")
                    else:
                        self._print("Reading body from {}...".format(getattr(inp, "name", "stream")))
                    if isinstance(body, s.reader):
                        parsed["_body"] = inp
                    else:
                        parsed["_body"] = _read(inp, body)
                name = None
                result = operation.function(**parsed)
            except s.SchemaError as se:
                if name:
                    se.push(_p2a(name))
                self._help_operation(resource_name, operation)
                raise
            self._print("SUCCESS.")
            if returns:
                description = (returns.description or "response.").lower()
                if out is not sys.stdout:
                    self._print("Writing response to {}...".format(getattr(out, "name", "stream")))
                if isinstance(returns, s.reader):
                    copyfileobj(result, out)
                    result.close()
                else:
                    _write(out, returns, result)
                if out is sys.stdout:
                    self._print()
        return True

    def _help_list(self):
        """List all available resources and commands."""
        self._print("Available resources:")
        resources = {k: self.resources[k].description for k in self.resources if k not in self.private} 
        self._print_listing(resources, indent="  ")
        self._print("Available commands:")
        commands = {k: self.commands[k][1] for k in self.commands if self.commands[k][1]}
        self._print_listing(commands, indent="  ")
        return False

    def _help_resource(self, resource_name, args=None):
        operation_name = _a2p(args.pop(0)) if args else None
        operation = self.resources[resource_name].operations.get(operation_name)
        if operation:
            return self._help_operation(resource_name, operation)
        self._print("Usage: {} operation [ARGS] [<INFILE] [>OUTFILE]".format(resource_name))
        self._print("  {}".format(self.resources[resource_name].description))
        self._print("Operations:")
        ops = self.resources[resource_name].operations.values()
        operations = {_p2a(o.name): o.summary for o in ops}
        self._print_listing(operations, indent="  ")
        return False

    def _help_operation(self, resource_name, operation):
        params = operation.params or {}
        usage=[]
        listing={}
        for name in (n for n in params if n != "_body"):
            param = params[name]
            munged = _p2a(name)
            arg = "--{}={}".format(munged, param.python_type.__name__.upper())
            item = param.description or ""
            if param.enum:
                item += "  {" + ",".join((param.str_encode(e) for e in sorted(param.enum))) + "}"
            if param.default is not None:
                item += "  (default: {})".format(param.str_encode(param.default))
            listing["--{}".format(munged)] = item
            if not param.required:
                arg = "[{}]".format(arg)
            usage.append(arg)
        self._print("Usage: {} {} {}".format(resource_name, _p2a(operation.name), " ".join(usage)))
        self._print("  {}".format(operation.summary))
        if listing:
            self._print("Arguments:")
            self._print_listing(listing, indent="  ")
        if "_body" in params:
            description = params["_body"].description
            if description:
                self._print("Body: {}".format(description))
        if operation.returns:
            description = operation.returns.description
            if description:
                self._print("Response: {}".format(description))
        return False
