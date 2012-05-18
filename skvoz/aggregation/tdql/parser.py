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

from datetime import datetime, timedelta
from collections import defaultdict

from skvoz.aggregation.tdql import functions
from skvoz.aggregation.tdql.tokenizer import *
from skvoz.aggregation.tdql.rpn import *

def _strip_plural(symbol):
    if symbol.endswith('s'):
        return symbol[:-1]
    return symbol

class StmtSyntaxError(Exception):
    pass

class Stmt(object):
    def __init__(self):
        self.tokens = []

    def close(self):
        pass

    def add(self, token, symbol):
        self.tokens.append((token, symbol))

    def __repr__(self):
        return '%s %r' % (self.__class__.__name__, self.tokens)

class StmtFrom(Stmt):
    """
    FROM SOURCE a, b, c

        FROM FILES 'path/file' as k

    """
    SEPARATORS = (TOKEN_COMMA, TOKEN_PARENTHESES_OPEN, TOKEN_PARENTHESES_CLOSE)

    def __init__(self):
        self.keys = defaultdict(set)
        self.source = None
        self._key_name = False
        self._key = None

    def close(self):
        if self._key_name:
            raise StmtSyntaxError("Missing key name for '%s'!" % self._key)

        if self._key is not None:
            self.keys[self._key].add(self._key)
            self._key = None

    def add(self, token, symbol):
        if token in self.SEPARATORS:
            return

        if token == TOKEN_KEYWORD and symbol.lower() == 'as':
            if self.source is None:
                raise StmtSyntaxError("You need to specify a source!")
            if self._key is None:
                raise StmtSyntaxError("You need to specify a key or path!")
            self._key_name = True
            return

        if self.source is None:
            sym = _strip_plural(symbol.lower())
            self.source = sym
        elif self._key_name:
            self.keys[symbol].add(self._key)
            self._key_name = False
            self._key = None
        else:
            if self._key is not None:
                self.keys[self._key].add(self._key)
            self._key = symbol

    def __repr__(self):
        return 'From %s %r' % (self.source, dict(self.keys))

class StmtTime(Stmt):
    """
    TIME 10, 20 and 30, 40
    TIME 5 months
    TIME 10 days
    TIME 5 months 15 days
    """
    SEPARATORS = (TOKEN_COMMA, TOKEN_PARENTHESES_OPEN, TOKEN_PARENTHESES_CLOSE)

    FORMATS = [
        "%Y",
        "%Y-%m",
        "%Y-%m-%d",
        "%Y-%m-%d-%H",
        "%Y-%m-%d-%H:%M",
        "%Y-%m-%d-%H:%M:%S",
    ]

    def __init__(self):
        self.start = None
        self.end = None

    def add(self, token, symbol):
        if token in self.SEPARATORS:
            return

        if token == TOKEN_KEYWORD:
            sym = '_last_' + symbol.lower()
            if not sym.endswith('s'): sym += 's'

            if hasattr(self, sym):
                func = getattr(self, sym)
                if self.end is not None:
                    self.end = func(self.end)
                else:
                    self.start = func(self.start)
                return

        if isinstance(symbol, basestring):
            symbol = self._from_string(symbol)

        if self.start is None:
            self.start = symbol
        elif self.end is None:
            self.end = symbol
        else:
            raise StmtSyntaxError("Time interval is just start-end!")

    def __repr__(self):
        return 'Time Interval %r-%r' % (self.start, self.end)

    @staticmethod
    def _last_years(n):
        return datetime.today().year - n

    @staticmethod
    def _last_months(n):
        today = datetime.today()
        first_of_month = today - timedelta(days=(today.day - 1))
        tref = first_of_month - timedelta(days=(30 * n))
        return datetime(tref.year, tref.month, 1)

    @staticmethod
    def _last_weeks(n):
        today = datetime.today()
        weekday = today.strftime('%W')
        for d in xrange(7):
            if weekday != (today - timedelta(days=(d + 1))):
                break
        return today - timedelta(days=(d + (7 * n)))

    @staticmethod
    def _last_days(n):
        return datetime.today() - timedelta(days=n)

    @staticmethod
    def _last_hours(n):
        return datetime.today() - timedelta(hours=n)

    @staticmethod
    def _last_minutes(n):
        return datetime.today() - timedelta(minutes=n)

    @staticmethod
    def _last_seconds(n):
        return datetime.today() - timedelta(seconds=n)

    @staticmethod
    def _from_string(ds):
        for format in StmtTime.FORMATS:
            try:
                return datetime.strptime(ds, format)
            except ValueError:
                pass
        raise Exception("Could not determine date from %s" % ds)

class StmtSplit(Stmt):
    """
    SPLIT a, b, c ON ':'
    """
    def __init__(self):
        self.results = []
        self.delimiters = None

    def add(self, token, symbol):
        if token == TOKEN_COMMA:
            return

        if token == TOKEN_KEYWORD and symbol.lower() == 'on':
            self.delimiters = []
            return

        if self.delimiters is None:
            self.results.append(symbol)
        else:
            self.delimiters.append(symbol)

    def __repr__(self):
        return 'Split %r On %r' % (self.results, self.delimiters)

class StmtGroupBy(Stmt):
    """
    GROUP BY key, month, year, ...
    """
    TIME_GROUPS = ('year', 'month', 'day', 'week', 'hour', 'minute')

    def __init__(self):
        self.time_period = None
        #self.splits = set()
        self.key = False

    def __contains__(self, key):
        return key in self.groups

    def add(self, token, symbol):
        symbol = symbol.lower()
        if token == TOKEN_COMMA or (token == TOKEN_KEYWORD and symbol == 'by'):
            return

        symbol = _strip_plural(symbol)
        if symbol == 'key':
            self.key = True
        elif symbol in self.TIME_GROUPS:
            if self.time_period is not None and symbol != self.time_period:
                raise StmtSyntaxError("Another time period already specified '%s'" % self.time_period)
            self.time_period = symbol
        else:
            #self.splits.add(symbol)
            raise StmtSyntaxError("Invalid group '%s'." % symbol)

    def __repr__(self):
        return 'Group By Key=%r Period %r' % (self.key, self.time_period)

class StmtWhere(Stmt):
    def __init__(self):
        self.clauses = None
        self._infix2rpn = InfixToRpn()

    def close(self):
        result = self._infix2rpn.evaluated_rpn()
        self._infix2rpn = None
        if isinstance(result, list):
            self.clauses = result

    def add(self, token, symbol):
        self._infix2rpn.add(token, symbol)

    def evaluator(self):
        return RpnBooleanEvaluator(self.clauses)

    def __repr__(self):
        return 'Where %r' % self.clauses

class StmtFunction(Stmt):
    FUNCTIONS = ('min', 'max', 'avg', 'sum')
    def __init__(self):
        self.content = InfixToRpn()
        self.functions = {}

    def close(self):
        self.content = self.content.evaluated_rpn()

    def add(self, token, symbol):
        if token == TOKEN_KEYWORD:
            func_name = symbol.lower()
            agfn = func_name.capitalize() + 'Function'
            if hasattr(functions, agfn):
                if len(self.functions) > 0:
                    # TODO: Create two function category aggregation funcs and non
                    #       to be able to do: avg(sin(x) + 2 * cos(y))
                    raise StmtSyntaxError("Multi func not supported yet!")
                self.functions[func_name] = getattr(functions, agfn)()
                token = TOKEN_OPERATOR
        assert isinstance(self.content, InfixToRpn), "Function already closed!"
        self.content.add(token, symbol)

    def is_null(self):
        assert isinstance(self.content, InfixToRpn)
        return self.content.is_null()

    def is_valid(self):
        return self.content.is_valid()

    def __repr__(self):
        return rpn_to_infix_string(self.content)

class StmtStore(Stmt):
    def __init__(self):
        self.results = {}
        self._result_name = None
        self._current_function = StmtFunction()

    def close(self):
        self.add_function(self._current_function)
        self._current_function = None

    def add(self, token, symbol):
        # STORE f(a), f(b) as c
        if token == TOKEN_KEYWORD and symbol.lower() == 'as':
            self._result_name = True
            return

        if self._result_name == True:
            self.add_function(self._current_function, symbol)
            return

        if token == TOKEN_COMMA:
            if self._current_function.is_null():
                return

            if self._current_function.is_valid():
                self.add_function(self._current_function)
                return

        self._current_function.add(token, symbol)

    def add_function(self, function, name=None):
        if not function.is_null():
            function.close()
            if not name: name = str(function)
            self.results[name] = function
            self._current_function = StmtFunction()
            self._result_name = None

    def functions(self):
        for key, func in self.results.iteritems():
            yield key, functions.Executor(func.functions, func.content)

    def __repr__(self):
        return 'Store %r' % self.results

class Query(object):
    STMT_TOKENS = {
        'from': StmtFrom,
        'time': StmtTime,
        'group': StmtGroupBy,
        'split': StmtSplit,
        'where': StmtWhere,
        'store': StmtStore,
    }

    def __init__(self):
        self.stmt_from = None
        self.stmt_time = None
        self.stmt_group = None
        self.stmt_split = None
        self.stmt_where = None
        self.stmt_store = None

    def __iter__(self):
        yield self.stmt_from
        if self.stmt_time is not None: yield self.stmt_time
        if self.stmt_group is not None: yield self.stmt_group
        if self.stmt_split is not None: yield self.stmt_split
        if self.stmt_where is not None: yield self.stmt_where
        if self.stmt_store is not None: yield self.stmt_store

    @classmethod
    def parse(cls, query):
        stmt = None
        query_obj = cls()
        for token, sym in tokenize(query):
            if token == TOKEN_KEYWORD and sym.lower() in cls.STMT_TOKENS:
                if stmt is not None: stmt.close()

                name = sym.lower()
                stmt_name = 'stmt_' + name

                if getattr(query_obj, stmt_name, None) is not None:
                    raise StmtSyntaxError("'%s' Statament Already specified" % name)

                stmt = cls.STMT_TOKENS[name]()
                setattr(query_obj, stmt_name, stmt)
            else:
                try:
                    stmt.add(token, sym)
                except StmtSyntaxError:
                    raise
                except Exception, e:
                    if __debug__: print 'SYNTAX-ERROR', e # TODO: REMOVE ME
                    raise StmtSyntaxError("Invalid syntax around symbol '%s'." % sym)

        if stmt is not None:
            stmt.close()
        if query_obj.stmt_from is None:
            raise StmtSyntaxError("Missing 'FROM' statament.")

        if query_obj.stmt_split is None:
            if query_obj.stmt_where is not None:
                raise StmtSyntaxError("You need to specify SPLIT to apply WHERE clauses.")

            if query_obj.stmt_store is not None:
                raise StmtSyntaxError("You need to specify SPLIT to STORE something.")

        # TODO: Check Store func args with Split vars
        return query_obj

if __name__ == '__main__':
    #print rpn_to_infix_string(rpn_evaluate(InfixToRpn.parse(funky_tokenizer("min(x)"))))
    #print rpn_evaluate(InfixToRpn.parse(funky_tokenizer("max((10 + 20))")))
    #import sys
    #sys.exit(1)

    def dump_stmts(query):
        print query
        for stmt in Query.parse(query):
            print stmt
        print

    dump_stmts("FROM FILES a, b, c TIME ('2011', '2012-5') GROUP BY key, months SPLIT a, b, c ON ':' STORE avg(c)")
    dump_stmts("FROM FILES a, b, c TIME 5 months 4 days GROUP BY key, months SPLIT a, b, c ON ':' STORE avg(a) as average, sum(b) as total")
    dump_stmts("FROM KEYS k, l, m TIME 5 months 4 days GROUP BY key, months SPLIT a, b, c ON ':' STORE avg(a) as average, sum(b) as total")
    dump_stmts("FROM KEYS k, l SPLIT a, b WHERE a > 20 AND b > (18 + 21 * (54 - 10))")
    dump_stmts("FROM KEYS k, l SPLIT a, b WHERE a b c")
    dump_stmts("FROM FILES 'test/a.txt' as ka, 'test/a2.txt' as ka 'test/b.txt' as b SPLIT a, b STORE min(b + 20 + 1) as b")

