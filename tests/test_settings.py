# -*- coding: utf-8 -*-

from tardis.test_settings import *


INSTALLED_APPS += (
    "mytardis_hsm",
)

# MIDDLEWARE_CLASSES = MIDDLEWARE_CLASSES + ('tardis.tardis_portal.filters.FilterInitMiddleware',)
# FILTER_MIDDLEWARE = (("tardis.tardis_portal.filters", "FilterInitMiddleware"),)
#
#
# HSM_MIN_FILE_SIZE = 500
#
#
# POST_SAVE_FILTERS = [
#    ("mytardis_hsm.filters.make_filter",
#    ["HSMFilter", "http://tardis.edu.au/hsm/1"]),
# ]
