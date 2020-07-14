"""
Contains mapping from Subject code to its canonical name in DTU academic department.
"""

# Mapping from code to subject of all courses offered in DTU.
from __future__ import absolute_import, division

all_courses = {}

import collections

# Declaring namedtuple()
Subject = collections.namedtuple('Subject', ['name', 'max_marks', 'credits', ])

# Mathematics and Computing courses.
mc_courses = {
    'MC-301': Subject('MODERN ALGEBRA',                  100, 4),
    'MC-302': Subject('OPERATIONS RESEARCH',             100, 4),
    'MC-303': Subject('FINANCIAL ENGINEERING',           100, 4),
    'MC-304': Subject('INTERNET & NETWORK SECURITY',     100, 4),
    'MC-305': Subject('DATABASE MANAGEMENT SYSTEM',      100, 4),
    'MC-306': Subject('DATABASE MANAGEMENT SYSTEM LAB',  100, 2),
    'MC-307': Subject('OPERATIONS RESEARCH LAB',         100, 2),
    'MC-308': Subject('INTERNET & NETWORK SECURITY LAB', 100, 2),
    'MC-309': Subject('MINOR PROJECT-I',                 200, 4)
}
all_courses.update(mc_courses)

# Environment Engineering Courses.
env_courses = {
    'EN-301': Subject('WATER SUPPLY & ENVIRONMENTAL SANITATION',    100, 4),
    'EN-302': Subject('HEAVY METAL REMOVALS ',                      100, 4),
    'EN-303': Subject('GEOTECHNICAL ENGINEERING',                   100, 4),
    'EN-304': Subject('ENVIRONMENTAL HYDRAULICS',                   100, 4),
    'EN-305': Subject('INSTRUMENTATION',                            100, 4),
    'EN-306': Subject('ENVIRONMENTAL HYDRAULICS LAB',               100, 2),
    'EN-307': Subject('GEOTECHNICAL ENGINEERING LAB',               100, 2),
    'EN-308': Subject('INSTRUMENTATION LAB ',                       100, 2),
    'EN-309': Subject('MINOR PROJECT-I /SURVEYING CAMP EVALUATION', 200, 4),
}
all_courses.update(env_courses)
