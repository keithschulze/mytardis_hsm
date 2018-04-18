# -*- coding: utf-8 -*-

"""mytardis_hsm.filters module which houses post-save filters for
adding HSM status metadata to Datafiles in MyTardis"""

from mytardis_hsm.tasks import create_df_status


class HSMFilter(object):
    """HSM post-save filters for adding HSM status metadata to Datafiles

    Attributes
    ----------
    name: str
        Short name for filter
    schema: str
        Name of the schema to load the extracted data into.
    """
    def __init__(self, name, schema):
        self.name = name
        self.schema = schema

    def __call__(self, sender, **kwargs):
        """Post save call back to invoke this filter.

        Parameters
        ----------
        sender: Model
            class of the model
        instance: model Instance
            Instance of model being saved (in kwargs).
        created: boolean
            Specifies whether a new record is being created
            (in kwargs).
        """
        instance = kwargs.get('instance')

        create_df_status.apply_async(
            args=[instance, self.schema]
        )


def make_filter(name, schema):
    """Factory method to instantiate HSMFilter instance"""
    return HSMFilter(name, schema)
