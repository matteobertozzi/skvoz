#!/usr/bin/env python
#
# Copyright (c) 2012, Matteo Bertozzi
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#    * Redistributions of source code must retain the above copyright
#      notice, this list of conditions and the following disclaimer.
#    * Redistributions in binary form must reproduce the above copyright
#     notice, this list of conditions and the following disclaimer in the
#     documentation and/or other materials provided with the distribution.
#   * Neither the name of the <organization> nor the
#     names of its contributors may be used to endorse or promote products
#     derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

from skvoz.aggregation.tdql.tokenizer import *

def _rpn_binary_evaluate(operation, operand_left, operand_right, context=None):
    token_left, operand_left = operand_left
    token_right, operand_right = operand_right
    if token_left == TOKEN_OPERATOR: raise Exception("Left operand is an operator")
    if token_right == TOKEN_OPERATOR: raise Exception("Left operand is an operator")
    #print 'BINARY', operand_left, operation, operand_right, context

    if token_left == TOKEN_KEYWORD: operand_left = context[operand_left]
    if token_right == TOKEN_KEYWORD: operand_right = context[operand_right]

    if operation == '>': return TOKEN_BOOLEAN, operand_left > operand_right
    if operation == '<': return TOKEN_BOOLEAN, operand_left < operand_right
    if operation == '<=': return TOKEN_BOOLEAN, operand_left <= operand_right
    if operation == '>=': return TOKEN_BOOLEAN, operand_left >= operand_right
    if operation == '==': return TOKEN_BOOLEAN, operand_left == operand_right
    if operation == '!=': return TOKEN_BOOLEAN, operand_left != operand_right

    if operation == 'AND': return TOKEN_BOOLEAN, operand_left and operand_right
    if operation == 'OR': return TOKEN_BOOLEAN, operand_left or operand_right

    # Numeric Only Operations
    if isinstance(operand_left, basestring) or isinstance(operand_left, basestring):
        raise Exception("String type not supported '%r %s %r'" % (operand_left, operation, operand_right))

    if operation == '+': return TOKEN_NUMBER, operand_left + operand_right
    if operation == '-': return TOKEN_NUMBER, operand_left - operand_right

    if operation == '*': return TOKEN_NUMBER, operand_left * operand_right
    if operation == '/': return TOKEN_NUMBER, operand_left / operand_right
    if operation == '%': return TOKEN_NUMBER, operand_left % operand_right

    if operation == '&': return TOKEN_NUMBER, operand_left & operand_right
    if operation == '|': return TOKEN_NUMBER, operand_left | operand_right
    if operation == '^': return TOKEN_NUMBER, operand_left ^ operand_right

    if operation == '<<': return TOKEN_NUMBER, operand_left << operand_right
    if operation == '>>': return TOKEN_NUMBER, operand_left >> operand_right

    raise Exception("Binary operation not handled '%r %r %r'" % (operand_left, operation, operand_right))

def _rpn_unary_evaluate(operation, operand, context=None):
    #print 'UNARY', operation, operand
    operand_token, operand = operand

    if operand_token == TOKEN_KEYWORD: operand = context[operand]

    if operation == '-': return -operand

    raise Exception("Unary operation not handled '%r' on '%r'" % (operation, operand))

def _rpn_function_evaluate(function, args, context):
    #print 'FUNCTION', function, args
    function = context[function]
    assert hasattr(function, '__call__')

    args = [rpn_evaluate(arg, context)[0][1] for arg in args]
    return function(*args)

def rpn_evaluate(expr_tokens, context=None):
    op_stack = []
    for token, symbol in expr_tokens:
        if token == TOKEN_FUNCTION:
            try:
                fargs = op_stack.pop()
                r = _rpn_function_evaluate(symbol, fargs[1], context)
                op_stack.append(r)
            except:
                op_stack.append(fargs)
                op_stack.append((token, symbol))
        elif token == TOKEN_OPERATOR:
            if len(op_stack) > 1:
                try:
                    op_right = op_stack.pop()
                    op_left = op_stack.pop()
                    r = _rpn_binary_evaluate(symbol, op_left, op_right, context)
                    op_stack.append(r)
                except:
                    op_stack.append(op_left)
                    op_stack.append(op_right)
                    op_stack.append((token, symbol))
            else:
                try:
                    op = op_stack.pop()
                    r = _rpn_unary_evaluate(symbol, op, context)
                    op_stack.append(r)
                except:
                    op_stack.append(op)
                    op_stack.append((token, symbol))
        else:
            op_stack.append((token, symbol))

    if context is not None:
        for i, (token, symbol) in enumerate(op_stack):
            if token == TOKEN_KEYWORD:
                op_stack[i] = (token, context.get(symbol, symbol))

    return op_stack

def rpn_to_infix_string(expr_tokens):
    stack = []
    for token, symbol in expr_tokens:
        if token == TOKEN_FUNCTION:
            fargs = stack.pop()
            stack.append('%s(%s)' % (symbol, ', '.join([rpn_to_infix_string(a) for a in fargs])))
        elif token == TOKEN_OPERATOR:
            if len(stack) > 1:
                op_right = stack.pop()
                op_left = stack.pop()
                stack.append('(%s %s %s)' % (op_left, symbol, op_right))
            else:
                op = stack.pop()
                stack.append('%s(%s)' % (symbol, op))
        else:
            stack.append(symbol)

    if len(stack) > 1:
        raise Exception("Invalid syntax")

    return str(stack[-1])

class RpnEvaluator(object):
    def __init__(self, rpn, context=None):
        self.context = context
        self.rpn = rpn

    def evaluate(self, items):
        context = items if self.context is None else dict(self.context, **items)
        return rpn_evaluate(self.rpn, context)

    def __call__(self, items):
        return self.evaluate(items)

class RpnBooleanEvaluator(RpnEvaluator):
    def __call__(self, items):
        r = self.evaluate(items)
        if len(r) != 1 or isinstance(r[0], basestring):
            q = rpn_to_infix_string(self.rpn)
            raise Exception("Evaluation fail on %r, rpn result %r" % (items, q))
        return not bool(r[0][1])

class InfixToRpn(object):
    OPERATORS_PRECEDENCE = (
        ('NOT'),
        ('*', '/', '%'),
        ('+', '-'),
        ('<<', '>>'),
        ('<', '>','<=', '>='),
        ('!=', '=='),
        ('&', ),
        ('^', ),
        ('|', ),
        ('AND', ),
        ('OR', ),
    )

    def __init__(self, functions=[]):
        self.stack = []
        self.count = 0
        self.rpn_tokens = []
        self.func_context = []
        self.functions = functions

    def rpn(self):
        stack = self.stack
        while len(stack) > 0:
            self.rpn_tokens.append(stack.pop())
        return self.rpn_tokens

    def evaluated_rpn(self):
        return rpn_evaluate(self.rpn())

    def is_null(self):
        return (len(self.stack) + len(self.rpn_tokens)) == 0

    def is_valid(self):
        return self.count == 0

    def add(self, token, symbol):
        stack = self.stack

        if len(self.func_context) > 0:
            fctx = self.func_context[-1]
            if not (token == TOKEN_PARENTHESES_CLOSE and fctx.count == 0):
                if token == TOKEN_COMMA:
                    stack.append(fctx.rpn())
                    self.func_context[-1] = InfixToRpn(self.functions)
                else:
                    fctx.add(token, symbol)
                return

        if token == TOKEN_OPERATOR:
            while len(stack) > 0 and stack[-1][0] == TOKEN_OPERATOR:
                if self._cmp_precedence(symbol, stack[-1][1]) <= 0:
                    self.rpn_tokens.append(stack.pop())
                    continue
                break
            stack.append((token, symbol))
        elif token == TOKEN_PARENTHESES_OPEN:
            if len(self.rpn_tokens) > 0 and self.rpn_tokens[-1][1] in self.functions:
                self.func_context.append(InfixToRpn(self.functions))

            self.count += 1
            stack.append((token, symbol))
        elif token == TOKEN_PARENTHESES_CLOSE:
            self.count -= 1
            if len(self.func_context) > 0:
                fctx = self.func_context.pop()
                args = [fctx.rpn()]
                while len(stack) > 0 and stack[-1][0] != TOKEN_PARENTHESES_OPEN:
                    args.insert(0, stack.pop())
                _, sym = self.rpn_tokens.pop()
                self.rpn_tokens.append((TOKEN_FUNCTION_ARGS, args))
                self.rpn_tokens.append((TOKEN_FUNCTION, sym))
            else:
                while len(stack) > 0 and stack[-1][0] != TOKEN_PARENTHESES_OPEN:
                    self.rpn_tokens.append(stack.pop())
            stack.pop()
        else:
            self.rpn_tokens.append((token, symbol))

    def _cmp_precedence(self, token1, token2):
        p1 = None
        p2 = None
        for i, ops in enumerate(self.OPERATORS_PRECEDENCE):
            if p1 is None and token1 in ops:
                p1 = len(self.OPERATORS_PRECEDENCE) - i
                if p2 is not None: break
            if p2 is None and token2 in ops:
                p2 = len(self.OPERATORS_PRECEDENCE) - i
                if p1 is not None: break
        else:
            if p1 is None: p1 = len(self.OPERATORS_PRECEDENCE)
            if p2 is None: p2 = len(self.OPERATORS_PRECEDENCE)
        return p1 - p2

    @classmethod
    def parse(cls, expr_tokens, functions=None):
        infix2rpn = InfixToRpn(functions)
        for token, symbol in expr_tokens:
            if token == TOKEN_KEYWORD and symbol.upper() in functions:
                token = TOKEN_OPERATOR
            infix2rpn.add(token, symbol)
        return infix2rpn.rpn()

if __name__ == '__main__':
    def _ident(x): return TOKEN_NUMBER, x
    def _sum(a, b): return TOKEN_NUMBER, a + b

    functions = {'func': _ident, 'sum': _sum}
    context = {'a': 10, 'b': 20}
    context.update(functions)
    expr = 'func(a) + 2'
    expr = 'sum(a, func(b) + 2) * 2'
    print rpn_to_infix_string(rpn_evaluate(InfixToRpn.parse(tokenize(expr), functions.keys()), context))

