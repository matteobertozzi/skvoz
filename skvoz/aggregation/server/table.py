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

from collections import defaultdict

class Table(object):
    def __init__(self, name, columns):
        self.columns = list(columns)
        self.name = name
        self.rows = []

    def count(self):
        return len(self.rows)

    def insert(self, values):
        assert len(values) == len(self.columns)
        if isinstance(values, dict):
            values = [values[k] for k in self.columns]
        self.rows.append(values)

    def bulk_insert(self, lvalues):
        for values in lvalues:
            self.insert(values)

    def __len__(self):
        return len(self.rows)

    def __iter__(self):
        for row in self.rows:
            yield dict((k, v) for k, v in zip(self.columns, row))

class JoinTable(Table):
    def __init__(self, table_a, table_b):
        self.table_a = table_a
        self.table_b = table_b
        name = '%s+%s' % (table_a.name, table_b.name)
        columns_a = ['%s.%s' % (table_a.name, c) for c in table_a.columns]
        columns_b = ['%s.%s' % (table_b.name, c) for c in table_b.columns]
        super(JoinTable, self).__init__(name, columns_a + columns_b)

    def insert(self, values_a, values_b):
        assert isinstance(values_a, dict)
        assert isinstance(values_b, dict)

        values_a = [values_a[k] for k in self.table_a.columns]
        values_b = [values_b[k] for k in self.table_b.columns]
        assert len(values_a + values_b) == len(self.columns)
        return super(JoinTable, self).insert(values_a + values_b)

def cross_join(table_a, table_b):
    """
    Cross join returns the Cartesian product of rows from tables in the join.
    In other words, it will produce rows which combine each row from the first
    table with each row from the second table.
    """
    results = JoinTable(table_a, table_b)
    for b_row in table_b:
        for a_row in table_a:
            results.insert(a_row, b_row)
    return results

def inner_join(table_a, table_b, predicate_func):
    """
    Inner join creates a new result table by combining column values of two
    tables (A and B) based upon the join-predicate.
    The query compares each row of A with each row of B to find all pairs of
    rows which satisfy the join-predicate. When the join-predicate is satisfied,
    column values for each matched pair of rows of A and B are combined into a
    result row.
    """
    results = JoinTable(table_a, table_b)
    for a_row in table_a:
        for b_row in table_b:
            if predicate_func(a_row, b_row):
                results.insert(a_row, b_row)
    return results

def equi_join(table_a, table_b, key_a, key_b):
    """
    An equi-join is a specific type of comparator-based join, or theta join,
    that uses only equality comparisons in the join-predicate.
    """
    return inner_join(table_a, table_b, lambda a, b: a[key_a] == b[key_b])

def natural_join(table_a, table_b):
    """
    A natural join offers a further specialization of equi-joins.
    The join predicate arises implicitly by comparing all columns in both
    tables that have the same column-names in the joined tables.
    """
    keys = set(table_a.columns) & set(table_b.columns)
    predicate = lambda a, b: all([a[k] == b[k] for k in keys])
    return inner_join(table_a, table_b, predicate)

def left_outer_join(table_a, table_b, predicate_func):
    """
    The result of a left outer join (or simply left join) for table A and B
    always contains all records of the "left" table (A), even if the
    join-condition does not find any matching record in the "right" table (B).
    This means that if the ON clause matches 0 (zero) records in B, the join
    will still return a row in the result-but with NULL in each column from B.
    This means that a left outer join returns all the values from the left
    table, plus matched values from the right table (or NULL in case of no
    matching join predicate). If the right table returns one row and the left
    table returns more than one matching row for it, the values in the right
    table will be repeated for each distinct row on the left table
    """
    null_b_row = dict((k, None) for k in table_b.columns)

    results = JoinTable(table_a, table_b)
    for a_row in table_a:
        match = False
        for b_row in table_b:
            if predicate_func(a_row, b_row):
                results.insert(a_row, b_row)
                match = True
        if match == False:
            results.insert(a_row, null_b_row)
    return results

def right_outer_join(table_a, table_b, predicate_func):
    """
    A right outer join (or right join) closely resembles a left outer join,
    except with the treatment of the tables reversed.
    Every row from the "right" table (B) will appear in the joined table
    at least once. If no matching row from the "left" table (A) exists, NULL
    will appear in columns from A for those records that have no match in B.
    """
    return left_outer_join(table_b, table_a, predicate_func)

def group_by(table, keys):
    groups = defaultdict(list)
    for row in table:
        gkey = tuple((k, row.pop(k)) for k in keys)
        groups[gkey].append(row)

    columns = [col for col in table.columns if col not in keys]
    for key, rows in sorted(groups.iteritems()):
        tgroups = Table(None, columns)
        tgroups.bulk_insert(rows)
        yield dict(key), tgroups

