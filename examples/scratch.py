# Write random but useful code snippets in this file.
import collections
import json


def get_err_stats():
    # Run this from root dir
    """
    >>> [print(f"{k:70}:{v}") for k,v in get_err_stats().items()]
    KeyError('name')                                                      :569
    tabula found 0 pages                                                  :327
    Couldn't determine 'branch'                                           :156
    AttributeError("'NoneType' object has no attribute 'groups'")         :66
    Couldn't determine 'examination_date'                                 :7
    KeyError('[0] not found in axis')                                     :26
    KeyError('papers_failed')                                             :27
    IndexError('pop from empty list')                                     :5
    KeyError('SPI')                                                       :43
    Couldn't determine 'program'                                          :9
    AttributeError("'NoneType' object has no attribute 'split'")          :2
    KeyError('TC')                                                        :1
    """
    with open("etc/parse_progress.json", "r") as f:
        parse_progress = json.load(f)

    t={}
    for k, err in parse_progress.items():
        if err is True:
            continue
        if err.startswith("ValueError(\"Couldn\'t determine"):
            t[k] = err[12:err.find(" from")]
        elif "tabula found 0 pages" in err:
            t[k] = "tabula found 0 pages"
        else:
            t[k] = err

    errs = collections.defaultdict(list)
    for k,v in t.items():
        errs[v].append(k)

    return {k:len(v) for k,v in errs.items()}
