#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os

import yamlconf

from rest.stasis.StasisClient import StasisClient


class TestStasisClient:
    @classmethod
    def setup_class(cls):
        with open(os.path.join(os.path.dirname(__file__),
                               '..', '..', 'appconfig.yml'), 'r') as conf:
            cls.config = yamlconf.load(conf)

    def test_get_states(self):
        stasis_cli = StasisClient(self.config['stasis']['url'])

        states = stasis_cli.get_states()

        assert all(x in states for x in ['entered', 'acquired', 'converted',
                                         'processing', 'deconvoluted', 'corrected',
                                         'annotated', 'quantified', 'replaced',
                                         'exported', 'failed'])
