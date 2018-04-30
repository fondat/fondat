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


def _print_listing(listing):
    """TODO: Description."""
    names = sorted(listing.keys())
    ljust = len(max(names, key=len)) + 2
    for name in names:
        print(name.ljust(ljust) + listing[name])    

def _arg_munge(name):
    """TODO: Description."""
    return "--{}".format(name.replace("_", "-"))

def _input_body():
    """TODO: Description."""
    print("Enter body, followed by EOF (^D on *nix, ^Z on Windows):")
    return sys.stdin.read()


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
        """
        usage: help [command | resource [operation]]

        Provide help with commands and resources.
        """
        name = args.pop(0) if args else None
        if not name:
            self._help_list()
        elif name in self.resources:
            self._help_resource(name, args)
        elif name in self.commands:
            for line in self.commands[name][0].__doc__.splitlines():
                print(line.lstrip())
            return False
        else:
            print('No help for "{}".'.format(name))

    def _exit(self, args):
        """
        usage: exit

        Exit the command line interface.
        """
        raise StopIteration()

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
                args.add_argument(_arg_munge(name), **kwargs)
        return parser

    def _process_resource(self, resource_name, args):
        """TODO: Description."""
        resource = self.resources[resource_name]
        operation_name = args.pop(0) if args else None
        operation = resource.operations.get(operation_name)
        params = operation["params"] or {}
        if not operation:
            return self._help_resource(resource_name)
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
                parsed["_body"] = _input_body()
            except SchemaError as se:
                print("{} {}: error: body: {}".format(resource_name, operation_name, se.msg))
                return False
        try:
            result = resource.call(operation_name, parsed)
        except ResourceException as re:
            print("status: failure: detail: {} code: {}".format(re.code, re.detail))
            return False
        except Exception as e:
            print("status: failure: detail: {}".format(e))
            return False
        if "returns" in operation:
            result = operation["returns"].str_encode(result)
        print("status: success")
        print(result)
        return True

    def _help_list(self):
        """TODO: Description."""
        listing = {}
        for name in self.commands:
            description = self.commands[name][1]
            if description:
                listing[name] = description
        for name, resource in self.resources.items():
            listing[name] = resource.description
        print()
        print("Available commands and resources:")
        print()
        _print_listing(listing)
        print()
        return False

    def _help_resource(self, resource_name, args=None):
        """TODO: Description."""
        operation = self.resources[resource_name].operations.get(args.pop(0)) if args else None
        if operation:
            self._help_operation(resource_name, operation)
        else:
            self._help_operations(resource_name)
        return False

    def _help_operation(self, resource_name, operation):
        """TODO: Description."""
        parser = self._build_parser(resource_name, operation)
        print()
        parser.print_help()
        print()
        return False

    def _help_operations(self, resource_name):
        """TODO: Description."""
        listing = {}
        for operation in self.resources[resource_name].operations.values():
            listing[operation["name"]] = operation["summary"]
            print()
            print("usage: {} operation [arguments]".format(resource_name))
            print()
            print(self.resources[resource_name].description)
            print()
            print("Operations:")
            print()
            _print_listing(listing)
            print()
        return False
