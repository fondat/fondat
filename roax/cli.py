"""Module to expose resources through a command-line interface."""

import io
import json
import re
import readline
import roax.context
import roax.schema as s
import shlex
import shutil
import sys
import traceback


_re = re.compile("(_*)(.*)(_*)")


def _p2a(name):
    m = _re.match(name)
    return m.group(1) + m.group(2).replace("_", "-") + m.group(3)


def _a2p(name):
    m = _re.match(name)
    return m.group(1) + m.group(2).replace("-", "_") + m.group(3)


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
    if isinstance(inp, io.TextIOWrapper):
        inp = inp.buffer
    return schema.bin_decode(inp.read())


def _write(out, schema, value):
    if isinstance(out, io.TextIOWrapper):
        out = out.buffer
    out.write(schema.bin_encode(value))
    out.flush()


def _summary(function):
    """Return a summary line of text from usage docstring."""
    return function.__doc__.splitlines()[1].lstrip().rstrip()


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


class _Exit(Exception):
    pass


class CLI:
    """
    Command line interface that exposes registered resources.

    Parameters and instance variables:
    • name: The name of the application.
    • debug: Print details for any raised exceptions.
    • err: Output stream for writing prompts and errors.
    • prefix: Prefix for parameters.
    • log: Log function to write log information.
    """

    def __init__(
        self, name=None, *, debug=False, err=sys.stderr, prefix="--", log=None
    ):
        super().__init__()
        self.name = name
        self.debug = debug
        self.err = err
        self.prefix = prefix
        self.log = log
        self.resources = {}
        self.commands = {}
        self.hidden = set()
        self._register_commands()

    def _check_not_registered(self, name):
        if name in self.resources:
            raise ValueError(f"{name} is already a registered resource")
        if name in self.commands:
            raise ValueError(f"{name} is already a registered command")

    def register_resource(self, name, resource, hidden=False):
        """
        Register a resource with the command line interface.

        Parameters:
        • name: The name to expose for the resource via command line.
        • resource: The resource to be registered.
        • hidden: Hide the resource in help listings.
        """
        self._check_not_registered(name)
        self.resources[name] = resource
        if hidden:
            self.hidden.add(name)

    def register_command(self, name, function, hidden=False):
        """
        Register a command with the command line interface.

        Parameters:
        • name: The name to expose for the command via command line.
        • function: The function to call when command is invoked.
        • hidden: Hide the command in help listings.

        The command's docstring (__doc__) is required to have its usage on the first
        line, the summary description on the second line, and any further help
        documentation on subsequent lines.
        
        The command function requires an args parameter to accept arguments passed to
        it from the command line.
        """
        self._check_not_registered(name)
        self.commands[name] = function
        if hidden:
            self.hidden.add(name)

    def loop(self, prompt=None):
        """
        Repeatedly issue a command prompt and process input.

        Parameter:
        • prompt: The prompt to display for each command.
        
        The prompt can be a string or a callable to return a string containing the
        prompt to display.
        """
        prompt = prompt or f'{self.name or ""}> '
        while True:
            try:
                self.process(input(prompt() if callable(prompt) else prompt))
            except (EOFError, KeyboardInterrupt):
                self._print()
                break
            except _Exit:
                break

    def process(self, line, inp=sys.stdin, out=sys.stdout):
        """
        Process a single command line.
        
        Parameters:
        • line: Command line string to process.

        Returns:
        `True` if command line was processed successfully.
        """
        try:
            if self.log:
                self.log("%s", (line,))
            args = shlex.split(line)
            if not args:
                return True
            with roax.context.push(context="cli", command=line):
                name = args.pop(0)
                if name in self.resources:
                    return self._process_resource(name, args, inp, out)
                elif name in self.commands:
                    return self.commands[name](args)
                else:
                    self._print(f"Invalid command or resource: {name}.")
                    return False
        except _Exit:
            raise
        except Exception as e:
            if self.log:
                self.log("%s", (e,), exc_info=self.debug)
            self._print(f"ERROR: {e}")
            if self.debug:
                traceback.print_exc()
            return False

    def _register_commands(self):
        self.register_command("help", self._help)
        self.register_command("exit", self._exit)
        self.register_command("quit", self._exit, hidden=True)
        self.register_command("q", self._exit, hidden=True)
        self.register_command("debug", self._debug, hidden=True)

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
        self._print(f"Unrecognized resource or command: {name}.")
        return False

    def _exit(self, args):
        """\
        Usage: exit
          Exit the command line interface.\
        """
        raise _Exit

    def _debug(self, args):
        """\
        Usage: debug [on|off]
          Enable, disable or print debugging status.\
        """
        if args and args[0] == "on":
            self.debug = True
        elif args and args[0] == "off":
            self.debug = False
        elif args:
            self._help_command("debug")
            return False
        print(f'Debugging status: {"on" if self.debug else "off"}.')
        return True

    def _print(self, *args, **varargs):
        if self.err:
            print(*args, file=self.err, **varargs)

    def _print_listing(self, listing, indent="", space=4, max_column=24):
        """Sort a dictionary by key and print as a listing."""
        names = sorted(listing.keys())
        ljust = 0
        for name in names:
            if len(name) <= max_column and len(name) > ljust:
                ljust = len(name)
        for name in names:
            self._print(f'{indent}{name.ljust(ljust)}{" " * space}{listing[name]}')

    def _help_command(self, name):
        """Print the function docstring of a command as help text."""
        self._print(textwrap.dedent(self.commands[name].__doc__))
        return False

    def _parse_arguments(self, params, args):
        """Parse arguments for supported operation parameters."""
        result = {}
        args = list(args)
        name = None
        while args:
            arg = args.pop(0)
            if name is None:
                if not arg.startswith(self.prefix):
                    raise ValueError
                arg = arg[len(self.prefix) :]
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
        if name:  # parameter name supplied without value
            raise ValueError
        return result

    def _process_resource(self, resource_name, args, inp, out):
        """Process a command for a resource."""
        resource = self.resources[resource_name]
        operation_name = _a2p(args.pop(0)) if args else None
        operation = resource.operations.get(operation_name)
        if not operation:
            return self._help_resource(resource_name)
        params = operation.params
        returns = operation.returns
        body = params.get("_body")
        with _open_redirects(inp, out, args, body, returns) as (inp, out):
            try:
                parsed = self._parse_arguments(params, args)
            except ValueError:
                return self._help_operation(resource_name, operation)
            try:
                for name in parsed:
                    parsed[name] = params[name].str_decode(parsed[name])
                for name in params.properties:
                    if (
                        name != "_body"
                        and name in params.required
                        and name not in parsed
                    ):
                        raise s.SchemaError("missing required parameter")
                if body:
                    name = "{body}"
                    description = (body.description or f"{name}.").lower()
                    if inp == sys.stdin:
                        self._print(f"Enter {description}")
                        self._print(
                            "When complete, input EOF (*nix: Ctrl-D, Windows: Ctrl-Z+Return):"
                        )
                    else:
                        self._print(
                            f'Reading body from {getattr(inp, "name", "stream")}...'
                        )
                    if isinstance(body, s.reader):
                        parsed["_body"] = inp
                    else:
                        parsed["_body"] = _read(inp, body)
                name = None
                result = operation.call(**parsed)
            except s.SchemaError as se:
                if name:
                    se.push(_p2a(name))
                self._help_operation(resource_name, operation)
                raise
            self._print("SUCCESS.")
            if returns:
                description = (returns.description or "response.").lower()
                if out is not sys.stdout:
                    self._print(
                        f'Writing response to {getattr(out, "name", "stream")}...'
                    )
                if isinstance(returns, s.reader):
                    shutil.copyfileobj(result, out)
                    result.close()
                else:
                    _write(out, returns, result)
                if out is sys.stdout:
                    self._print()
        return True

    def _help_list(self):
        """List all available resources and commands."""
        self._print("Available resources:")
        resources = {
            k: self.resources[k].description
            for k in self.resources
            if k not in self.hidden
        }
        self._print_listing(resources, indent="  ")
        self._print("Available commands:")
        commands = {
            k: _summary(self.commands[k]) for k in self.commands if k not in self.hidden
        }
        self._print_listing(commands, indent="  ")
        return False

    def _help_resource(self, resource_name, args=None):
        """Provide operations that are available for a specific resource."""
        operation_name = _a2p(args.pop(0)) if args else None
        operation = self.resources[resource_name].operations.get(operation_name)
        if operation:
            return self._help_operation(resource_name, operation)
        self._print(f"Usage: {resource_name} operation [ARGS] [<INFILE] [>OUTFILE]")
        self._print(f"  {self.resources[resource_name].description}")
        self._print("Operations:")
        ops = self.resources[resource_name].operations.values()
        operations = {_p2a(o.name): o.summary for o in ops}
        self._print_listing(operations, indent="  ")
        return False

    def _help_operation(self, resource_name, operation):
        """Provide detailed help message for specific operation."""
        params = operation.params
        usage = []
        listing = {}
        for name in (n for n in params if n != "_body"):
            param = params[name]
            munged = _p2a(name)
            arg = f"{self.prefix}{munged}={param.python_type.__name__.upper()}"
            item = param.description or ""
            if param.enum:
                item += (
                    "  {"
                    + ",".join((param.str_encode(e) for e in sorted(param.enum)))
                    + "}"
                )
            if param.default is not None:
                item += f"  (default: {param.str_encode(param.default)})"
            listing[f"{self.prefix}{munged}"] = item
            if name not in params.required:
                arg = f"[{arg}]"
            usage.append(arg)
        self._print(f'Usage: {resource_name} {_p2a(operation.name)} {" ".join(usage)}')
        self._print(f"  {operation.summary}")
        if listing:
            self._print("Arguments:")
            self._print_listing(listing, indent="  ")
        if "_body" in params:
            description = params["_body"].description
            if description:
                self._print(f"Body: {description}")
        if operation.returns:
            description = operation.returns.description
            if description:
                self._print(f"Response: {description}")
        return False
