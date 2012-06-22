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

from cStringIO import StringIO

SYMBOLS_QUOTE = ('"', "'")
SYMBOLS_SPACE = ' \t\r\n'

TOKEN_STRING = 1
TOKEN_NUMBER = 2
TOKEN_BOOLEAN = 3
TOKEN_KEYWORD = 4
TOKEN_OPERATOR = 5
TOKEN_FUNCTION = 6
TOKEN_FUNCTION_ARGS = 7
TOKEN_PARENTHESES_OPEN = 8
TOKEN_PARENTHESES_CLOSE = 9
TOKEN_COMMA = 10

TOKEN_SYMBOLS_TABLE = {
    None: SYMBOLS_SPACE,
    TOKEN_PARENTHESES_OPEN: '(',
    TOKEN_PARENTHESES_CLOSE: ')',
    TOKEN_COMMA: ',',
    TOKEN_OPERATOR: '+-*/%',
}

def _sdata_to_token(sdata):
    if not sdata:
        return None

    token = ''.join(sdata)
    for t in (int, float):
        try:
            return TOKEN_NUMBER, t(token)
        except ValueError:
            pass

    utoken = token.upper()
    if utoken == 'TRUE':
        return TOKEN_BOOLEAN, True
    if utoken == 'FALSE':
        return TOKEN_BOOLEAN, False
    if utoken in ('AND', 'OR', 'NOT'):
        return TOKEN_OPERATOR, utoken
    return TOKEN_KEYWORD, token

def tokenize(query):
    if isinstance(query, basestring):
        query = StringIO(query)

    quoted = None
    sdata = []

    while True:
        c = query.read(1)
        if not c: break

        if quoted:
            if c == quoted:
                yield TOKEN_STRING, ''.join(sdata)
                sdata = []
                quoted = None
            else:
                if c == '\\':
                    c = query.read(1)
                    if not c: break
                sdata.append(c)
            continue

        if c in SYMBOLS_QUOTE:
            quoted = c
            continue

        for sym_token, sym in TOKEN_SYMBOLS_TABLE.iteritems():
            if c in sym:
                stoken = _sdata_to_token(sdata)
                sdata = []
                if stoken is not None: yield stoken
                if sym_token is not None: yield sym_token, c
                break
        else:
            if c in '><=!':
                stoken = _sdata_to_token(sdata)
                sdata = []
                if stoken is not None: yield stoken

                nc = query.read(1)
                if not c:break

                if nc == '=':
                    yield TOKEN_OPERATOR, c + nc
                elif (c == '<' and nc in '<') or (c == '>' and nc == '>'):
                    yield TOKEN_OPERATOR, c + nc
                else:
                    if c in '=!': c += '='
                    yield TOKEN_OPERATOR, c
                    if nc not in SYMBOLS_SPACE:
                        sdata.append(nc)
            else:
                sdata.append(c)

    if quoted:
        raise Exception("Missing end quote")

    stoken = _sdata_to_token(sdata)
    if stoken is not None: yield stoken
