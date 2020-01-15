"""
Contains mapping from Subject code to its canonical name in DTU academic department.
"""

# Mapping from code to subject of all courses offered in DTU.
all_courses = {}

# Mathematics and Computing courses.
mc_courses = {
    'MC-301': 'MODERN ALGEBRA',
    'MC-302': 'OPERATIONS RESEARCH',
    'MC-303': 'FINANCIAL ENGINEERING',
    'MC-304': 'INTERNET & NETWORK SECURITY',
    'MC-305': 'DATABASE MANAGEMENT SYSTEM',
    'MC-306': 'DATABASEANAGEMENT SYSTEM LAB',
    'MC-307': 'OPERATIONS RESEARCH LAB',
    'MC-308': 'INTERNET & NETWORK SECURITY LAB',
    'MC-309': 'MINOR PROJECT-I',
}
all_courses.update(mc_courses)

# Environment Engineering Courses.
env_courses = {
    'EN-301': 'WATER SUPPLY & ENVIRONMENTAL SANITATION',
    'EN-302': 'HEAVY METAL REMOVALS ',
    'EN-303': 'GEOTECHNICAL ENGINEERING',
    'EN-304': 'ENVIRONMENTAL HYDRAULICS ',
    'EN-305': 'INSTRUMENTATION ',
    'EN-306': 'ENVIRONMENTAL HYDRAULICS LAB',
    'EN-307': 'GEOTECHNICAL ENGINEERING LAB ',
    'EN-308': 'INSTRUMENTATION LAB ',
    'EN-309': 'MINOR PROJECT-I /SURVEYING CAMP EVALUATION',
}
all_courses.update(env_courses)



