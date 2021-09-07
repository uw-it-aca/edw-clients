# Copyright 2021 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

import logging
import pandas as pd
import pymssql
from django.conf import settings


class BaseDAO():

    def __init__(self, *args, **kwargs):
        self.configure_pandas()

    def configure_pandas(self):
        """
        Configure global pandas options
        """
        pd.options.mode.use_inf_as_na = True
        pd.options.display.max_rows = 500
        pd.options.display.precision = 3
        pd.options.display.float_format = '{:.3f}'.format

    def get_connection(self, database):
        password = getattr(settings, "EDW_PASSWORD")
        user = getattr(settings, "EDW_USER")
        server = getattr(settings, "EDW_SERVER")
        conn = pymssql.connect(server, user, password, database)
        logging.debug(f"Connected to {server}.{database} with user {user}")
        return conn