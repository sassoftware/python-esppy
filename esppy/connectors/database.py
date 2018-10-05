#!/usr/bin/env python
# encoding: utf-8
#
# Copyright SAS Institute
#
#  Licensed under the Apache License, Version 2.0 (the License);
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

''' ESP Database Connector '''

from __future__ import print_function, division, absolute_import, unicode_literals

import numbers
import re
import six
from .base import Connector, prop, map_properties
from ..utils import xml
from ..utils.data import gen_name


class DatabaseSubscriber(Connector):
    '''
    Subscribe to database events

    Parameters
    ----------
    connectstring : string
        Specifies the database DSN and user credentials in the
        format 'DSN=dsn;uid=userid;pwd=password;'
    desttablename : string
        Specifies the target table name
    snapshot : boolean, optional
        Specifies whether to send snapshot data
    configfilesection : string, optional
        Specifies the name of the section in the config file to parse for
        configuration parameters. Specify the value as [configfilesection].
    commitrows : int, optional
        Specifies the minimum number of output rows to buffer
    commitsecs : int, optional
        Specifies the maximum number of seconds to hold onto an incomplete
        commit buffer
    ignoresqlerrors : boolean, optional
        Enables the connector to continue to write Inserts, Updates, and
        Deletes to the database table despite an error in a previous Insert,
        Update, or Delete.
    maxcolbinding : int, optional
        Specifies the maximum supported width of string columns.
        The default value is 4096.
    pwdencrypted : boolean, optional
        Specifies that the pwd field in connectstring is encrypted
    rmretdel : boolean, optional
        Specifies to remove all delete events from event blocks received
        by a subscriber that were introduced by a window retention policy.

    Returns
    -------
    :class:`DatabaseSubscriber`

    '''
    connector_key = dict(cls='db', type='subscribe')

    property_defs = dict(
        connectstring=prop('connectstring', dtype='string', required=True),
        desttablename=prop('desttablename', dtype='string', required=True),
        snapshot=prop('snapshot', dtype='boolean', required=True),
        configfilesection=prop('configfilesection', dtype='string'),
        commitrows=prop('commitrows', dtype='int'),
        commitsecs=prop('commitsecs', dtype='int'),
        ignoresqlerrors=prop('ignoresqlerrors', dtype='boolean'),
        maxcolbinding=prop('maxcolbinding', dtype='int'),
        pwdencrypted=prop('pwdencrypted', dtype='boolean'),
        rmretdel=prop('rmretdel', dtype='boolean')
    )

    def __init__(self, connectstring=None, desttablename=None, name=None, is_active=None,
                 snapshot=None, configfilesection=None, commitrows=None,
                 commitsecs=None, ignoresqlerrors=None, maxcolbinding=None,
                 pwdencrypted=None, rmretdel=None):
        params = dict(**locals())
        params.pop('is_active')
        params.pop('self')
        name = params.pop('name')
        Connector.__init__(self, 'db', name=name, type='subscribe',
                           is_active=is_active, properties=params)

    @classmethod
    def from_parameters(cls, conncls, type=None, name=None, is_active=None,
                        properties=None):
        req, properties = map_properties(cls, properties,
                                         required=['connectstring',
                                                   'desttablename'],
                                         delete='type')
        return cls(req[0], req[1], name=name, is_active=is_active, **properties)


class DatabasePublisher(Connector):
    '''
    Subscribe events to a database

    Parameters
    ----------
    connectstring : string
        Specifies the database DSN and user credentials in the format
        'DSN=dsn;uid=userid;pwd=password;'
    blocksize : int, optional
        Specifies the number of events to include in a published event
        block. The default value is 1.
    configfilesection : string, optional
        Specifies the name of the section in the config file to parse for
        configuration parameters. Specify the value as [configfilesection].
    greenplumlogminer : boolean, optional
        Enables Greenplum log miner mode
    logminerdbname : string, optional
        Specifies the gpperfmon database that contains the queries_history
        table for Greenplum log miner mode. Use the following
        format: 'dd-mmm-yyy hh:mm:ss'.
    logminerschemaowner : string, optional
        Specifies the schema owner when using Oracle or Greenplum log
        miner mode.
    logminerstartdatetime : string, optional
        Specifies the start date time when using Oracle or Greenplum
        log miner mode.
    logminertablename : string, optional
        Specifies the table name when using Oracle or Greenplum log
        miner mode.
    maxcolbinding : int, optional
        Specifies the maximum supported width of string columns.
        The default value is 4096.
    maxevents : int, optional
        Specifies the maximum number of events to publish.
    oraclelogminer : boolean, optional
        Enables Oracle log miner mode
    publishwithupsert : boolean, optional
        Builds events with opcode=Upsert instead of Insert
    pwdencrypted : boolean, optional
        Specifies that the pwd field in connectstring is encrypted
    selectstatement : string, optional
        Specifies the SQL statement to be executed on the source database.
        Required when oraclelogminer and greenplumlogminer are not enabled.
    transactional : string, optional
        Sets the event block type to transactional. The default value is normal.

    Returns
    -------
    :class:`DatabasePublisher`

    '''
    connector_key = dict(cls='db', type='publish')

    property_defs = dict(
        connectstring=prop('connectstring', dtype='string', required=True),
        blocksize=prop('blocksize', dtype='int'),
        configfilesection=prop('configfilesection', dtype='string'),
        greenplumlogminer=prop('greenplumlogminer', dtype='boolean'),
        logminerdbname=prop('logminerdbname', dtype='string'),
        logminerschemaowner=prop('logminerschemaowner', dtype='string'),
        logminerstartdatetime=prop('logminerstartdatetime', dtype='string'),
        logminertablename=prop('logminertablename', dtype='string'),
        maxcolbinding=prop('maxcolbinding', dtype='int'),
        maxevents=prop('maxevents', dtype='int'),
        oraclelogminer=prop('oraclelogminer', dtype='boolean'),
        publishwithupsert=prop('publishwithupsert', dtype='boolean'),
        pwdencrypted=prop('pwdencrypted', dtype='boolean'),
        selectstatement=prop('selectstatement', dtype='string'),
        transactional=prop('transactional', dtype='string') 
    )

    def __init__(self, connectstring=None, name=None, is_active=None, blocksize=None,
                 configfilesection=None, greenplumlogminer=None,
                 logminerdbname=None, logminerschemaowner=None,
                 logminerstartdatetime=None, logminertablename=None,
                 maxcolbinding=None, maxevents=None, oraclelogminer=None,
                 publishwithupsert=None, pwdencrypted=None,
                 selectstatement=None, transactional=None):
        params = dict(**locals())
        params.pop('is_active')
        params.pop('self')
        name = params.pop('name')
        Connector.__init__(self, 'db', name=name, type='publish',
                           is_active=is_active, properties=params)

    @classmethod
    def from_parameters(cls, conncls, type=None, name=None, is_active=None,
                        properties=None):
        req, properties = map_properties(cls, properties,
                                         required=['connectstring'],
                                         delete='type')
        return cls(req[0], name=name, is_active=is_active, **properties)
