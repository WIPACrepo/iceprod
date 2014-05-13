"""
  A recursive descent parser for the IceProd meta language
  
  copyright (c) 2013 the icecube collaboration
"""

import re
import string
import random

class safe_eval:
    """Safe evaluation for arithmatic operations"""
    import ast
    import operator as op
    # supported operators
    operators = {ast.Add: op.add,
                 ast.Sub: op.sub,
                 ast.Mult: op.mul,
                 ast.Div: op.truediv,
                 ast.Mod: op.mod,
                 ast.Pow: op.pow,
                 ast.BitXor: op.xor}
    @classmethod
    def eval(cls,expr):
        return cls.__eval(cls.ast.parse(expr).body[0].value) # Module(body=[Expr(value=...)])
    @classmethod
    def __eval(cls,node):
        if isinstance(node, cls.ast.Num): # <number>
            return node.n
        elif isinstance(node, cls.ast.operator): # <operator>
            return cls.operators[type(node)]
        elif isinstance(node, cls.ast.BinOp): # <left> <operator> <right>
            return cls.__eval(node.op)(cls.__eval(node.left), cls.__eval(node.right))
        else:
            raise TypeError(node)

from iceprod.core import dataclasses

class GrammarException(Exception):
    pass

class ExpParser:
    """
    Expression parsing class for parameter values. 
    """
    
    # grammar definition
    #
    # char      := 0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!"#%&'*+,-./:;<=>?@\^_`|~[]{}
    # word      := char | char + word
    # starter   := $
    # scopeL    := (
    # scopeR    := )
    # symbol    := starter | starter + word
    # phrase    := symbol + scopeL + sentence + scopeR
    # sentence  := phrase | word | phrase + sentence | word + sentence

    def parse(self,input,job=dataclasses.Job(),env={}):
        # only parse strings
        if not isinstance(input,str):
            return input
        # set job and env
        self.job = job
        self.env = env
        self.depth = 0 # start at a depth of 0
        # parse input
        (left, right) = self.sentence(input)
        # unset job and env
        self.job = None
        self.env = None
        # return parsed output
        return left + right

    def sentence(self,input):
        # find words or phrases
        if self.depth > 100:
            raise GrammarError('too recursive, check for circular dependencies')
        self.depth += 1
        try:
            (left,right) = self.phrase(input)
        except GrammarException:
            try:
                (left,right) = self.word(input)
            except:
                (left,right) = ('',input)
        if left and right:
            (left2,right) = self.sentence(right)
            left += left2
        if not right:
            right = ''
        if not left:
            left = ''
        self.depth -= 1
        return (left,right)
    
    def phrase(self,input):
        # should be symbol + scopeL + sentence + scopeR
        (sym,right) = self.symbol(input)
        (sL,right) = self.scopeL(right)
        (sen,right) = self.sentence(right)
        (sR,right) = self.scopeR(right)
        
        # do actual processing
        ret = self.process_phrase(sym,sen)
        
        # return processed work + input to the right
        return (ret,right)
    
    def symbol(self,input):
        # should be starter | starter + word
        (sym,right) = self.starter(input)
        try:
            (left,right) = self.word(right)
        except GrammarException:
            return (sym,right)
        else:
            return (sym + left,right)

    def scopeL(self,input):
        if input and input[0] in '(':
            return (input[0],input[1:])
        else:
            raise GrammarException('missing scopeL')

    def scopeR(self,input):
        if input and input[0] in ')':
            return (input[0],input[1:])
        else:
            raise GrammarException('missing scopeR')

    def starter(self,input):
        if input and input[0] == '$':
            return (input[0],input[1:])
        else:
            raise GrammarException('missing symbol starter')

    special_chars = set('$()')
    chars = set([x for x in string.printable if x not in special_chars])
    def word(self,input):
        i = 0
        l = len(input)
        special_chars = self.special_chars
        chars = self.chars
        while i < l:
            if input[i] == '\\' and i+1 < l and input[i+1] in special_chars:
                l -= 1
                input = input[:i] + input[i+1:]
            elif input[i] not in chars:
                break
            i += 1
        if i == 0:
            raise GrammarException('no chars in word')
        return (input[:i],input[i:]) 

    # done with grammar
    # do actual work now

    def process_phrase(self,sym,sentence):
        """ symbol is $ + word
            sentence is reduced to word
        """
        if sym == '$':
            keyword = sentence
            param = None
        else:
            keyword = sym[1:]
            param = sentence
        # search for keyword in special list
        ret = None
        if keyword in self.keywords:
            try:
                ret = self.keywords[keyword](param)
            except GrammarException:
                pass
        if ret == None and param == None:
            # do general search for keyword
            if 'parameters' in self.env and keyword in self.env['parameters']: # search env params first
                ret = str(self.env['parameters'][keyword].value)
            elif hasattr(self.job,keyword): # search job second
                try:
                    ret = str(getattr(self.job,keyword))
                except:
                    pass
            elif keyword in self.job.options: # search options third
                try:
                    ret = str(self.job.options[keyword].value)
                except:
                    pass
        
        if ret == None:
            ret = sym+'('+sentence+')'
        else:
            ret = ''.join(self.sentence(ret))
        return ret
    
    def steering_func(self,param):
        """Find param in steering"""
        if self.job.steering and param in self.job.steering.parameters:
            return self.job.steering.parameters[param].value
        else:
            raise GrammarException('steering:'+str(param))

    def system_func(self,param):
        """Find param in steering.system"""
        if self.job.steering and param in self.job.steering.system:
            return self.job.steering.system[param].value
        else:
            raise GrammarException('system:'+str(param))
    
    def options_func(self,param):
        """Find param in options"""
        if param in self.job.options:
            return self.job.options[param].value
        else:
            raise GrammarException('options:'+str(param))
    
    def difplus_func(self,param):
        """Find param in dif plus"""
        try:
            # try dif
            return str(getattr(self.job.difplus.dif,param))
        except:
            try:
                return str(getattr(self.job.difplus.plus,param))
            except:
                raise GrammarException('difplus:'+str(param))
    
    def choice_func(self,param):
        """Evaluate param as choice expression"""
        try:
            return random.choice(param.split(','))
        except:
            raise GrammarException('not a valid choice')
    
    def eval_func(self,param):
        """Evaluate param as arithmetic expression"""
        #bad = re.search(r'(import)|(open)|(for)|(while)|(def)|(class)|(lambda)', param )
        bad = reduce(lambda a, b: a or (b in param),('import','open','for','while','def','class','lambda'),False)
        if bad:
            raise GrammarException('Unsafe operator call')
        else:
            try:
                return str(safe_eval.eval(param))
            except Exception as e:
                raise GrammarException('Eval is not basic arithmetic')
    
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
                if not fstring: return arg
                if fstring[-1] in 'cs':
                    if arg[0] in '\'"':            return str(arg[1:-1])
                    else:                          return str(arg)
                elif fstring[-1] == 'r':           return repr(arg)
                elif fstring[-1] in 'idu':         return int(arg)
                elif fstring[-1].lower() in 'feg': return float(arg)
                elif fstring[-1].lower() == 'x':   return int('0x%s' % arg, 0)
                elif fstring[-1] == 'o':           return int('0%s' % arg, 0)
                else:
                    raise GrammarException('Unable to cast %s using format %s'%(arg,fstring))
            
            fstrings = re.findall(r'\%[#0\- +]{0,1}[0-9]*\.{0,1}[0-9]*[csridufegExXo]',fmt_str)
            args = map(cast_string,fstrings,args)[0:len(args)]
            
            # do sprintf on fmt_str and args
            if len(args) > 0:
                return fmt_str % tuple(args)
            else:
                return fmt_str
        except Exception as e:
            raise GrammarException(str(e))
    
    def __init__(self):
        self.job = None
        self.env = None
        self.depth = 0
        # dict of keyword : function mappings
        self.keywords = {'steering' : self.steering_func,
                         'system' : self.system_func,
                         'args' : self.options_func,
                         'options' : self.options_func,
                         'metadata' : self.difplus_func,
                         'eval' : self.eval_func,
                         'choice' : self.choice_func,
                         'sprintf' : self.sprintf_func
                        }
