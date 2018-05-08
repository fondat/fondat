"""Module to expose resources through a command-line interface."""

# Copyright © 2018 Paul Bryan.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import argparse
import readline
import roax.schema as s
import shlex
import sys
import traceback

from roax.context import context
from roax.resource import ResourceError
from textwrap import dedent


def _print_listing(listing, indent="", space=4, max_column=24):
    """Sort a dictionary by key and print as a listing."""
    names = sorted(listing.keys())
    ljust = 0
    for name in names:
        if len(name) <= max_column and len(name) > ljust:
            ljust = len(name)
    for name in names:
        print("{}{}{}{}".format(indent, name.ljust(ljust), " " * space, listing[name]))    

def _is_binary(schema):
    return isinstance(schema, s.bytes) and schema.format == "binary"

def _parse_redirects(args, body_schema, returns_schema):
    """Parse redirections in command line arguments; removing them from arguments list."""    
    stdin, stdout = None, None
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
            raise ValueError("no redirection filename specified")
        if redir == "<" and not body_schema:
            raise ValueError("input redirection for operation that takes no body")
        if redir in (">", ">>") and not returns_schema:
            raise ValueError("output redirection for operation with no response")
        if redir == "<" and stdin:
            raise ValueError("more than one input file specified")
        elif redir in (">", ">>") and stdout:
            raise ValueError("more than one output file specified")
        if "<" in filename or ">" in filename:
            raise ValueError("invalid character in redirection filename")
        in_mode = "b" if body_schema and _is_binary(body_schema) else "t"
        out_mode = "b" if returns_schema and _is_binary(returns_schema) else "t"
        if redir == "<":
            stdin = open(filename, "r" + in_mode)
        elif redir == ">":
            stdout = open(filename, "w" + out_mode)
        elif redir == ">>":
            stdout = open(filename, "a" + out_mode)
    return stdin, stdout


class CLI:
    """Command line interface that exposes registered resources."""

    def __init__(self, *, name=None, prompt=None, debug=False):
        """
        Initialize a command line interface.

        :param name: The name of the application.
        :param prompt: The prompt to display for each command.
        :param debug: Display details for raised exceptions.
        """
        super().__init__()
        self.name = name or self.__class__.__name__
        self.prompt = prompt or name + "> "
        self.debug = debug
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
        try:
            while self._looping:
                try:
                    self.process(input(self.prompt))
                except SystemExit:  # argparse trying to terminate
                    pass
                except (EOFError, StopIteration, KeyboardInterrupt):
                    break
                except Exception as e:
                    print("ERROR: {}".format(e))
                    if self.debug:
                        traceback.print_exc()
        finally:
            self._looping = False

    def process(self, line):
        """Process a single command line."""
        args = shlex.split(line)
        if not args:
            return
        with context(type="cli", cli_command=line):
            name = args.pop(0)
            if name in self.resources:
                self._process_resource(name, args)
            elif name in self.commands:
                self.commands[name][0](args)
            else:
                print("Invalid command or resource: {}.".format(name))

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
        print("Unrecognized resource or command: {}.".format(name))
        return False

    def _exit(self, args):
        """\
        Usage: exit
          Exit the command line interface.\
        """
        raise StopIteration()

    def _help_command(self, name):
        """Print the function docstring of a command as help text."""
        print(dedent(self.commands[name][0].__doc__))
        return False

    def _build_parser(self, resource_name, operation):
        params = operation.params or {}
        body = params.get("_body", None)
        parser = argparse.ArgumentParser(
            prog = "{} {}".format(resource_name, operation.name),
            description = operation.summary,
            add_help = False,
            allow_abbrev = False,
        )
        args = parser.add_argument_group('arguments')
        if params:
            for name, schema in ((n, s) for n, s in params.items() if n != "_body"):
                kwargs = {}
                kwargs["required"] = schema.required
                description = schema.description
                if schema.enum is not None:
                    enum = (schema.str_encode(v) for v in schema.enum)
                    description += " [{}]".format("|".join(enum))
                if schema.default is not None:
                    description += " (default: {})".format(schema.str_encode(schema.default))
                kwargs["help"] = description
                kwargs["metavar"] = schema.python_type.__name__.lower()
                args.add_argument("--{}".format(name.replace("_","-")), **kwargs)
        return parser

    def _process_resource(self, resource_name, args):
        resource = self.resources[resource_name]
        operation_name = args.pop(0).replace("-", "_") if args else None
        operation = resource.operations.get(operation_name)
        if not operation:
            return self._help_resource(resource_name)
        params = operation.params or {}
        returns = operation.returns
        body = params.get("_body")
        stdin, stdout = _parse_redirects(args, params.get("_body"), returns)
        parser = self._build_parser(resource_name, operation)
        parsed = {k: v for k, v in vars(parser.parse_args(args)).items() if v is not None}
        for name in (n for n in parsed if n != "_body"):
            try:
                parsed[name] = params[name].str_decode(parsed[name])
            except s.SchemaError as se:
                print("ERROR: parameter {}: {}".format(name, se.msg))
                return False
        if body:
            try:
                description = (body.description or "content body.").lower()
                if stdin:
                    print("Redirecting body from file {}: {}".format(stdin.name, description))
                else:
                    print("Enter {}".format(description))
                    print("When complete, input EOF (^D on *nix, ^Z on Windows):")
                    stdin = sys.stdin.buffer if _is_binary(body) else sys.stdin
                decode = body.bin_decode if _is_binary(body) else body.str_decode
                parsed["_body"] = decode(stdin.read())
            except s.SchemaError as se:
                print("ERROR: {} {}: content body: {}".format(resource_name, operation_name, se.msg))
                return False
        try:
            result = operation.function(**parsed)
        except ResourceError as re:
            print("ERROR: {} (code: {}).".format(re.detail, re.code))
            return False
        except Exception as e:
            print("ERROR: {}.".format(e))
            return False
        print("SUCCESS.")
        if returns:
            description = (returns.description or "response.").lower()
            if stdout:
                print("Redirecting response to file {}: {}".format(stdout.name, description))
            else:
                stdout = sys.stdout.buffer if _is_binary(returns) else sys.stdout
            if _is_binary(returns):
                stdout.write(returns.bin_encode(result))
            else:
                print(returns.str_encode(result), file=stdout)
        return True

    def _help_list(self):
        """List all available resources and commands."""
        print("Available resources:")
        resources = {k: self.resources[k].description for k in self.resources if k not in self.private} 
        _print_listing(resources, indent="  ")
        print("Available commands:")
        commands = {k: self.commands[k][1] for k in self.commands if self.commands[k][1]}
        _print_listing(commands, indent="  ")
        return False

    def _help_resource(self, resource_name, args=None):
        operation_name = args.pop(0).replace("-", "_") if args else None
        operation = self.resources[resource_name].operations.get(operation_name)
        if operation:
            return self._help_operation(resource_name, operation)
        print("Usage: {} operation [ARGS] [<INFILE] [>OUTFILE]".format(resource_name))
        print("  {}".format(self.resources[resource_name].description))
        print("Operations:")
        ops = self.resources[resource_name].operations.values()
        operations = {o.name.replace("_", "-"): o.summary for o in ops}
        _print_listing(operations, indent="  ")

    def _help_operation(self, resource_name, operation):
        params = operation.params or {}
        usage=[]
        listing={}
        for name in (n for n in params if n != "_body"):
            param = params[name]
            munged = name.replace("_", "-")
            arg = "--{}={}".format(munged, param.python_type.__name__.upper())
            item = param.description or ""
            if param.enum:
                item += "  {" + ",".join((param.str_encode(e) for e in param.enum)) + "}"
            if param.default is not None:
                item += "  (default: {})".format(param.str_encode(param.default))
            listing["--{}".format(munged)] = item
            if not param.required:
                arg = "[{}]".format(arg)
            usage.append(arg)
        print("Usage: {} {} {}".format(resource_name, operation.name.replace("_", "-"), " ".join(usage)))
        print("  {}".format(operation.summary))
        if listing:
            print("Arguments:")
            _print_listing(listing, indent="  ")
        if "_body" in params:
            description = params["_body"].description
            if description:
                print("Body: {}".format(description))
        if operation.returns:
            description = operation.returns.description
            if description:
                print("Response: {}".format(description))
        return False
