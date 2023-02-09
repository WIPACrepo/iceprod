"""
A `recursive descent parser
<http://en.wikipedia.org/wiki/Recursive_descent_parser>`_ for the IceProd meta
language. Most commonly used in IceProd dataset configurations to refer to
other parts of the same configuration.
"""

from __future__ import absolute_import, division, print_function

import re
import random
import functools
import json
import builtins
import logging
import ast
import operator as op

from iceprod.core import dataclasses

logger = logging.getLogger('parser')


class safe_eval:
    # supported operators
    operators = {
        ast.Add: op.add,
        ast.Sub: op.sub,
        ast.Mult: op.mul,
        ast.Div: op.truediv,
        ast.FloorDiv: op.floordiv,
        ast.Mod: op.mod,
        ast.Pow: op.pow,
        ast.BitXor: op.xor,
        ast.Invert: op.invert,
        ast.Not: op.not_,
        ast.UAdd: op.pos,
        ast.USub: op.neg,
    }

    @classmethod
    def eval(cls,expr):
        """
        Safe evaluation of arithmatic operations using
        :mod:`Abstract Syntax Trees <ast>`.
        """
        return cls.__eval(ast.parse(expr).body[0].value)  # Module(body=[Expr(value=...)])

    @classmethod
    def __eval(cls,node):
        if isinstance(node, ast.Num):  # <number>
            return node.n
        elif isinstance(node, (ast.operator,ast.unaryop)):  # <operator>
            return cls.operators[type(node)]
        elif isinstance(node, ast.BinOp):  # <left> <operator> <right>
            return cls.__eval(node.op)(cls.__eval(node.left), cls.__eval(node.right))
        elif isinstance(node, ast.UnaryOp):  # <operator> <right>
            return cls.__eval(node.op)(cls.__eval(node.operand))
        else:
            raise TypeError(node)


class GrammarException(Exception):
    pass


def getType(output):
    try:
        if isinstance(output,dataclasses.String) and not (output.startswith('"') and output.endswith('"')):
            try:
                output = json.loads(output.replace("'",'"'))
            except Exception:
                logging.debug('error formatting json', exc_info=True)
                if output.lower() == 'true':
                    output = True
                elif output.lower() == 'false':
                    output = False
                elif output.isdigit():
                    output = int(output)
                else:
                    output = float(output)
    except Exception:
        pass
    return output


def parse_ret_type(ret):
    if isinstance(ret, (list,dict)):
        return ret
    else:
        return str(ret)


tokens = ["word", "name", "starter", "scopeL", "scopeR", "bracketL", "bracketR"]


def scanner(data):
    """A lexical scanner, yielding token pairs"""
    word = ''
    escape = False
    for ch in data:
        if escape:
            word += ch
            escape = False
        elif ch == '\\':
            escape = True
        elif ch in '$()[]':
            if word:
                yield ('word', word)
                word = ''
            if ch == '$':
                yield ('starter', '$')
            elif ch == '(':
                yield ('scopeL', '(')
            elif ch == ')':
                yield ('scopeR', ')')
            elif ch == '[':
                yield ('bracketL', '[')
            elif ch == ']':
                yield ('bracketR', ']')
        else:
            word += ch
    if word:
        yield ('word', word)


def parser(data):
    """A syntactic parser, yielding syntactically accurate token pairs"""
    last_token = None
    last_word = ''
    nestings = 0
    stack = []
    for token,word in scanner(data):
        logger.debug('%s,%s,%s,%s,%d,%r',token,word,last_token,last_word,nestings,stack)
        if token == 'starter':
            if last_token == 'word':
                yield ("word", last_word)
            if last_token:
                stack.append(nestings)
                nestings = 0
                last_word = ''
            yield ("starter", '$')
            last_token = 'starter'
        elif token == 'scopeL':
            if last_token == 'starter':
                yield ('name', None)
                last_token = 'name'
            if last_token == 'name':
                yield (token,word)
                last_token = 'scopeL'
                last_word = ''
            else:
                # must be part of a broken word
                if last_token == 'word':
                    last_word += word
                else:
                    last_token = 'word'
                    last_word = word
                nestings += 1
        elif token == 'scopeR':
            if last_token == 'word':
                if nestings > 0:
                    last_word += word
                    nestings -= 1
                else:
                    yield ("word", last_word)
                    yield (token,word)
                    if stack:
                        nestings = stack.pop()
                    else:
                        nestings = 0
                    last_word = ''
            elif last_token == 'scopeL':
                yield ("word", '')
                yield (token,word)
                last_token = 'word'
                last_word = ''
            else:
                raise SyntaxError()
        elif token == 'bracketL':
            if last_token == 'word':
                if last_word:
                    # part of a broken word
                    last_word += '['
                    nestings += 1
                else:
                    yield ('bracketL', '[')
                    last_token = 'bracketL'
            else:
                raise SyntaxError()
        elif token == 'bracketR':
            if last_token == 'word':
                if nestings > 0:
                    last_word += word
                    nestings -= 1
                else:
                    yield ("word", last_word)
                    yield (token,word)
                    if stack:
                        nestings = stack.pop()
                    else:
                        nestings = 0
                    last_word = ''
            elif last_token == 'bracketL':
                yield ("word", '')
                yield (token,word)
                last_token = 'word'
                last_word = ''
            else:
                raise SyntaxError()
        elif token == 'word':
            if last_token == 'starter':
                yield ('name', word)
                last_token = 'name'
            elif last_token == 'word':
                last_word += word
            else:
                last_token = 'word'
                last_word = word
        else:
            # bad token
            raise SyntaxError()
    if last_word:
        yield ("word", last_word)
    if nestings or stack:
        raise SyntaxError()


class ExpParser:
    """
    Expression parsing class for parameter values.

    Grammar Definition::

        char     := any unicode character other than $()[]
        word     := char | char + word
        starter  := $
        scopeL   := (
        scopeR   := )
        bracketL := [
        bracketR := ]
        symbol   := starter | starter + word
        phrase   := symbol + scopeL + sentence + scopeR
        lookup   := word + bracketL + word + bracketR | phrase + bracketL + word + bracketR
        sentence := lookup | phrase | word | lookup + sentence | phrase + sentence | word + sentence

    Keywords:

    * steering : A parameter from :class:`iceprod.core.dataclasses.Steering`
    * system : A system value from :class:`iceprod.core.dataclasses.Steering`
    * args, options : An option value from :class:`iceprod.core.dataclasses.Job`
    * metadata : A value from :class:`iceprod.core.dataclasses.Dif` or
      :class:`iceprod.core.dataclasses.Plus`
    * eval : An arithmatic expression
    * sum, min, max, len : Apply a reduction to a sequence
    * choice : A random choice from a list of possibilites
    * sprintf : The sprintf string syntax

    Examples::

        $steering(my_parameter)
        $system(gpu_opts)
        $args(option1)
        $options(option1)
        $metadata(sensor_name)
        $eval(1+2)
        $choice(1,2,3,4)
        $sprintf("%04d",4)
    """
    def __init__(self):
        self.job = None
        self.env = None
        self.depth = 0
        # dict of keyword : function mappings
        self.keywords = {
            'steering' : self.steering_func,
            'system' : self.system_func,
            'environ' : self.environ_func,
            'args' : self.options_func,
            'options' : self.options_func,
            'metadata' : self.difplus_func,
            'eval' : self.eval_func,
            'choice' : self.choice_func,
            'sprintf' : self.sprintf_func
        }
        for reduction in 'sum', 'min', 'max', 'len':
            self.keywords[reduction] = functools.partial(self.reduce_func, getattr(builtins, reduction))

    def parse(self,input,job=None,env=None,depth=20):
        """
        Parse the input, expanding where possible.

        :param input: input string
        :param job: :class:`iceprod.core.dataclasses.Job`, optional
        :param env: env dictionary, optional
        :param depth: how deep to recursively parse
        :returns: expanded string
        """
        if depth < 1:
            logger.warning("recursion depth of parse exceeded")
            return input

        logger.debug("parse: %r",input)
        if not isinstance(input,dataclasses.String) or not input:
            # check for lists or dicts to recurse into
            if isinstance(input, list):
                logger.debug("recursing into list: %r", input)
                input = [self.parse(x, job=job, env=env, depth=depth-1) for x in input]
            elif isinstance(input, dict):
                logger.debug("recursing into dict: %r", input)
                input = {self.parse(x, job=job, env=env, depth=depth-1):self.parse(input[x],job=job,env=env,depth=depth-1) for x in input}
            return input

        # set job and env
        if job:
            self.job = job
        else:
            self.job = dataclasses.Job()
        if env:
            self.env = env
        else:
            self.env = {}
        self.depth = 0  # start at a depth of 0

        while True:
            # parse input
            stack = []
            try:
                for token,word in parser(input):
                    logger.debug('exp %s,%s,%r',token,word,stack)
                    if token in ('starter','name','word','scopeL','bracketL'):
                        stack.append((token,word))
                    elif token == 'scopeR':
                        # coelsce stack up to scopeL
                        word = ''
                        while stack and stack[-1][0] != 'scopeL':
                            word = stack.pop()[1] + word
                        stack.pop()  # remove scopeL
                        name = stack.pop()[1]
                        stack.pop()  # remove starter

                        # try evaluating this
                        try:
                            args = []
                            if name:
                                args.append(name)
                            args.append(word)
                            ret = self.process_phrase(*args)
                            if isinstance(ret, (list, dict)):
                                ret = json.dumps(ret)
                            stack.append(('word',str(ret)))
                        except GrammarException:
                            logger.debug('GrammarException')
                            stack.append(('word','$'+(name if name else '')+'('+word+')'))
                    elif token == 'bracketR':
                        # coelsce stack up to bracketL
                        word = ''
                        while stack and stack[-1][0] != 'bracketL':
                            word = stack.pop()[1] + word
                        stack.pop()  # remove bracketL

                        # try evaluating this
                        if word.endswith(']'):
                            # nested bracket, so recurse
                            word = self.parse(word, job=job, env=env, depth=depth-1)
                        try:
                            ret = self.process_phrase(word)
                        except GrammarException:
                            ret = word

                        # coelsce words
                        word = ''
                        while (stack and stack[-1][0] == 'word'
                               and (not word.startswith('['))
                               and (not word.startswith('{'))):
                            word = stack.pop()[1] + word

                        # now do list/dict index
                        try:
                            innerType = getType(ret)
                            ret = getType(word)[innerType]
                            stack.append(('word',str(ret)))
                        except Exception:
                            logger.debug('cannot eval: %s[%s]', word, ret,
                                         exc_info=True)
                            stack.append(('word',word+'['+ret+']'))
                    else:
                        raise SyntaxError()
            except Exception:
                logger.debug('SyntaxError', exc_info=True)
                output = getType(input)
            else:
                logger.debug('joining stack: %r', ''.join(s[1] for s in stack))
                output = getType(''.join(s[1] for s in stack))
                if isinstance(output,dataclasses.String) and output != input:
                    logger.debug('reprocessing output: %r', output)
                    input = output
                    continue
            break

        # check for lists or dicts to recurse into
        if isinstance(output, (list,dict)):
            output = self.parse(output, job=job, env=env, depth=depth)

        # return parsed output
        logger.debug('parser out: %r',output)
        return output

    def process_phrase(self,keyword,param=None):
        # search for keyword in special list
        ret = None
        if keyword in self.keywords and param is not None:
            try:
                ret = self.keywords[keyword](param)
            except GrammarException:
                pass
        if ret is None and param is None:
            # do general search for keyword
            if 'parameters' in self.env and keyword in self.env['parameters']:
                # search env params first
                ret = self.env['parameters'][keyword]
            elif keyword in self.job and not isinstance(self.job[keyword],dict):
                # search job second
                try:
                    ret = self.job[keyword]
                except Exception:
                    pass
            elif keyword in self.job['options']:  # search options third
                try:
                    ret = self.job['options'][keyword]
                except Exception:
                    pass
            elif self.job['steering'] and keyword in self.job['steering']['parameters']:
                # search job steering last
                ret = self.job['steering']['parameters'][keyword]

        if ret is None:
            raise GrammarException()
        return parse_ret_type(ret)

    def steering_func(self,param):
        """Find param in steering"""
        if self.job['steering'] and param in self.job['steering']['parameters']:
            return parse_ret_type(self.job['steering']['parameters'][param])
        else:
            raise GrammarException('steering:'+param)

    def system_func(self,param):
        """Find param in steering.system"""
        if self.job['steering'] and param in self.job['steering']['system']:
            return parse_ret_type(self.job['steering']['system'][param])
        else:
            raise GrammarException('system:'+param)

    def environ_func(self,param):
        """Find param in env["environment"]"""
        if 'environment' in self.env and param in self.env['environment']:
            return parse_ret_type(self.env['environment'][param])
        else:
            raise GrammarException('environ:'+param)

    def options_func(self,param):
        """Find param in options"""
        if param in self.job['options']:
            return parse_ret_type(self.job['options'][param])
        else:
            raise GrammarException('options:'+param)

    def difplus_func(self,param):
        """Find param in dif plus"""
        try:
            # try dif, then plus
            return parse_ret_type(self.job['difplus']['dif'][param])
        except Exception:
            try:
                return parse_ret_type(self.job['difplus']['plus'][param])
            except Exception:
                raise GrammarException('difplus:'+param)

    def choice_func(self,param):
        """Evaluate param as choice expression"""
        if not param:
            raise GrammarException('no choices available')
        try:
            if isinstance(param,(tuple,list)):
                return parse_ret_type(random.choice(param))
            else:
                return parse_ret_type(random.choice(param.split(',')))
        except Exception:
            raise GrammarException('not a valid choice')

    def eval_func(self,param):
        """Evaluate param as arithmetic expression"""
        bad = functools.reduce(lambda a, b: a or (b in param),('import','open','for','while','def','class','lambda'),False)
        if bad:
            raise GrammarException('Unsafe operator call')
        else:
            try:
                return parse_ret_type(safe_eval.eval(param))
            except Exception:
                raise GrammarException('Eval is not basic arithmetic')

    def reduce_func(self, func, param):
        try:
            return parse_ret_type(func(getType(param)))
        except Exception:
            raise GrammarException('Not a reducible sequence')

    def sprintf_func(self,param):
        """Evaluate param as sprintf.  param = arg_str, arg0, arg1, ... """
        # separate into format string and args
        strchar = param[0]
        if strchar in '\'"':
            pos = param.find(strchar,1)
            if pos < 0:
                raise GrammarException("Can't find closing quote for format string")
            fmt_str = param[1:pos]
        else:
            pos = param.find(',',0)
            if pos < 0:
                raise GrammarException("Can't find end of format string")
            fmt_str = param[0:pos]
        args = []
        pos = param.find(',',pos)
        while pos >= 0:
            pos2 = param.find(',',pos+1)
            if pos2 < 0:
                args.append(param[pos+1:])
                break
            else:
                args.append(param[pos+1:pos2])
                pos = pos2

        try:
            # cast args to correct type
            def cast_string(fstring,arg):
                """cast string to value according to formatting character"""
                if not fstring:
                    return arg
                if fstring[-1] in 'cs':
                    if arg[0] in '\'"':
                        return str(arg[1:-1])
                    else:
                        return str(arg)
                elif fstring[-1] == 'r':
                    return repr(arg)
                elif fstring[-1].lower() in 'xo':
                    return int(arg)
                elif fstring[-1].lower() in 'idufeg':
                    return float(arg)
                else:
                    raise GrammarException('Unable to cast %s using format %s'%(arg,fstring))

            fstrings = re.findall(r'\%[#0\- +]{0,1}[0-9]*\.{0,1}[0-9]*[csridufegExXo]',fmt_str)
            args = list(map(cast_string,fstrings,args))[0:len(args)]

            # do sprintf on fmt_str and args
            if fstrings:
                return fmt_str % tuple(args)
            else:
                return fmt_str
        except Exception as e:
            raise GrammarException(str(e))
