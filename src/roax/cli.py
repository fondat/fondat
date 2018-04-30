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
    names = sorted(listing.keys())
    ljust = len(max(names, key=len)) + 4
    for name in names:
        print(name.ljust(ljust) + listing[name])    

def _usage(name, doc):
    print("{}: ".format(name), end="")
    for line in doc.splitlines():
        line = line.lstrip()
        if line:
            print(line)


def _arg_munge(name):
    return "--{}".format(name.replace("_", "-"))


def _input_body():
    print("Enter body, followed by EOF (^D on *nix, ^Z on Windows):")
    return sys.stdin.read()


class CLI:
    """TODO: Description."""

    def _init_commands(self):
        self.commands["help"] = (self.help, "Request help with commands and resources.")
        self.commands["exit"] = (self.exit, "Exit the {} command line interface.".format(self.name))
        self.commands["quit"] = (self.exit, None)  # alias
        self.commands["q"] = (self.exit, None)  # alias

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
        """Process a command line."""
        if not args:
            return
        name = args.pop(0)
        if name in self.resources:
            self.process_resource(name, args)
        elif name in self.commands:
            self.commands[name][0](args)
        else:
            print("Invalid command or resource: {}.".format(name))

    def build_parser(self, resource_name, operation):
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

    def process_resource(self, resource_name, args):
        resource = self.resources[resource_name]
        operation_name = args.pop(0) if args else None
        operation = resource.operations.get(operation_name)
        params = operation["params"] or {}
        if not operation:
            return self.help_resource(resource_name)
        parser = self.build_parser(resource_name, operation)
        parsed = {k: v for k, v in vars(parser.parse_args(args)).items() if v is not None}
        if "_body" in params:
            parsed["_body"] = _input_body()
        for name in parsed:
            try:
                parsed[name] = params[name].str_decode(parsed[name])
            except SchemaError as se:
                arg_name = "body" if name == "_body" else "argument {}".format(_arg_munge(name))
                print("{} {}: error: {}: {}".format(resource_name, operation_name, arg_name, se.msg))
                return
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

    def help(self, args):
        """
        Provide help with commands and resources.
        Usage: help [command | resource [operation]]
        """
        name = args.pop(0) if args else None
        if not name:
            self.help_list()
        elif name in self.resources:
            self.help_resource(name, args)
        elif name in self.commands:
            self.help_command(name, args)
        else:
            print('No help for "{}".'.format(name))

    def help_command(self, name, args):
        _usage(name, self.commands[name][0].__doc__)

    def help_list(self):
        listing = {}
        for name in self.commands:
            description = self.commands[name][1]
            if description:
                listing[name] = description
        for name, resource in self.resources.items():
            listing[name] = resource.description
        print("Available commands and resources:")
        print()
        _print_listing(listing)

    def help_resource(self, resource_name, args=None):
        operation = self.resources[resource_name].operations.get(args.pop(0)) if args else None
        if operation:
            self.help_operation(resource_name, operation)
        else:
            self.help_operations(resource_name)

    def help_operations(self, resource_name):
        listing = {}
        for operation in self.resources[resource_name].operations.values():
            listing[operation["name"]] = operation["summary"]
            print("usage: {} operation [arguments]".format(resource_name))
            print()
            print(self.resources[resource_name].description)
            print()
            print("operations:")
            _print_listing(listing)

    def help_operation(self, resource_name, operation):
        parser = self.build_parser(resource_name, operation)
        parser.print_help()

    def exit(self, args):
        """
        Exit the command line interface.
        Usage: exit
        """
        raise StopIteration()
