"""Module to expose resources through a command-line interface."""

# Copyright © 2018 Paul Bryan.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import argparse
import readline
import shlex
import sys

from roax.schema import SchemaError
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

def _arg_munge(name):
    """Turn parameter name into command line argument."""


class CLI:
    """TODO: Description."""

    def __init__(self, *, name=None, prompt=None, resources={}):
        """TODO: Description."""
        super().__init__()
        self.name = name or self.__class__.__name__
        self.prompt = prompt or name + "> "
        self.resources = resources
        self.commands = {}
        self._looping = False
        self._init_commands()

    def loop(self):
        """Repeatedly issue a command prompt and process input."""
        if self._looping:
            raise ValueError("already looping")
        self._looping = True
        try:
            while self._looping:
                try:
                    self.process(shlex.split(input(self.prompt)))
                except SystemExit:  # argparse trying to terminate
                    pass
                except (EOFError, StopIteration, KeyboardInterrupt):
                    break
        finally:
            self._looping = False

    def process(self, args):
        """Process a single command line."""
        if not args:
            return
        name = args.pop(0)
        if name in self.resources:
            self._process_resource(name, args)
        elif name in self.commands:
            self.commands[name][0](args)
        else:
            print("Invalid command or resource: {}.".format(name))

    def _init_commands(self):
        """TODO: Description."""
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
        """TODO: Description."""
        params = operation.get("params", {})
        body = params.get("_body", None)
        parser = argparse.ArgumentParser(
            prog = "{} {}".format(resource_name, operation["name"]),
            description = operation["summary"],
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
        """TODO: Description."""
        resource = self.resources[resource_name]
        operation_name = args.pop(0) if args else None
        operation = resource.operations.get(operation_name)
        if not operation:
            return self._help_resource(resource_name)
        params = operation["params"] or {}
        parser = self._build_parser(resource_name, operation)
        parsed = {k: v for k, v in vars(parser.parse_args(args)).items() if v is not None}
        for name in (n for n in parsed if n != "_body"):
            try:
                parsed[name] = params[name].str_decode(parsed[name])
            except SchemaError as se:
                print("{} {}: error: argument {}: {}".format(resource_name, operation_name, name, se.msg))
                return False
        if "_body" in params:
            try:
                description = params["_body"].description or "content body."
                print("Input {}".format(description.lower()))
                print("When complete, input EOF (^D on *nix, ^Z on Windows):")
                parsed["_body"] = sys.stdin.read()
            except SchemaError as se:
                print("{} {}: error: content body: {}".format(resource_name, operation_name, se.msg))
                return False
        try:
            result = resource.call(operation_name, parsed)
        except ResourceException as re:
            print("Status: FAILURE: detail: {} code: {}.".format(re.code, re.detail))
            return False
        except Exception as e:
            print("Status: FAILURE: detail: {}.".format(e))
            return False
        if "returns" in operation:
            result = operation["returns"].str_encode(result)
        print("Status: success.")
        print(result)
        return True

    def _help_list(self):
        """List all available resources and commands."""
        print("Available resources:")
        resources = {k: self.resources[k].description for k in self.resources} 
        _print_listing(resources, indent="  ")
        print("Available commands:")
        commands = {k: self.commands[k][1] for k in self.commands if self.commands[k][1]}
        _print_listing(commands, indent="  ")
        return False

    def _help_resource(self, resource_name, args=None):
        """TODO: Description."""
        operation = self.resources[resource_name].operations.get(args.pop(0)) if args else None
        if operation:
            return self._help_operation(resource_name, operation)
        print("Usage: {} operation [args]".format(resource_name))
        print("  {}".format(self.resources[resource_name].description))
        print("Operations:")
        operations = {o["name"]: o["summary"] for o in self.resources[resource_name].operations.values()}
        _print_listing(operations, indent="  ")

    def _help_operation(self, resource_name, operation):
        """TODO: Description."""
        params = operation.get("params") or {}
        usage=[]
        listing={}
        for name in (n for n in params if n != "_body"):
            param = params[name]
            munged = name.replace("_", "-")
            arg = "--{}={}".format(munged, param.python_type.__name__.upper())
            item = param.description or ""
            if param.enum:
                item += "  {" + "|".join((param.str_encode(e) for e in param.enum)) + "}"
            if param.default is not None:
                item += "  (default: {})".format(param.str_encode(param.default))
            listing["--{}".format(munged)] = item
            if not param.required:
                arg = "[{}]".format(arg)
            usage.append(arg)
        print("Usage: {} {} {}".format(resource_name, operation["name"], " ".join(usage)))
        print("  {}".format(operation["summary"]))
        if listing:
            print("Arguments:")
            _print_listing(listing, indent="  ")
        if "_body" in params:
            description = params["_body"].description
            if description:
                print("Body: {}".format(description))
        if operation.get("returns"):
            description = operation["returns"].description
            if description:
                print("Response: {}".format(description))
        return False
