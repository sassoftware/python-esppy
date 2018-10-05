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

''' ESP Teradata Connector '''

from __future__ import print_function, division, absolute_import, unicode_literals

import numbers
import re
import six
from .base import Connector, prop, map_properties
from ..utils import xml
from ..utils.data import gen_name


class TeradataSubscriber(Connector):
    '''
    Subscribe to Teradata operations

    Parameters
    ----------
    tdatatdpid : string
        Specifies the target Teradata server name
    desttablename : string
        Specifies the target table name
    tdatausername : string
        Specifies the user name for the user account on the target
        Teradata server.
    tdatauserpwd : string
        Specifies the user password for the user account on the target
        Teradata server.
    tdatamaxsessions : int
        Specifies the maximum number of sessions created by the TPT to
        the Teradata server.
    tdataminsessions : int
        Specifies the minimum number of sessions created by the TPT to
        the Teradata server.
    tdatadriver : string
        Specifies the operator: stream, update, or load.
    tdatainsertonly : boolean
        Specifies whether events in the subscriber event stream processing
        window are insert only. Must be true when using the load operator.
    snapshot : boolean
        Specifies whether to send snapshot data
    rmretdel : boolean, optional
        Removes all delete events from event blocks received by the
        subscriber that were introduced by a window retention policy.
    tdatabatchperiod : int, optional
        Specifies the batch period in seconds. Required when using the
        update operator, otherwise ignored.
    stage1tablename : string, optional
        Specifies the first staging table. Required when using the load
        operator, otherwise ignored.
    stage2tablename : string, optional
        Specifies the second staging table. Required when using the load
        operator, otherwise ignored.
    connectstring : string, optional
        Specifies the connect string used to access the target and
        staging tables. Use the form “DSN=dsnname;UID=userid;pwd=password”.
        Required when using the load operator, otherwise ignored.
    connectstring : string, optional
        Specifies the connect string used to access the target and
        staging tables. 
    tdatatracelevel : int, optional
        Specifies the trace level for Teradata messages written to the
        trace file in the current working directory.
    configfilesection : string, optional
        Specifies the name of the section in the connector config file
        to parse for configuration parameters. Specify the value
        as [configfilesection].
    tdatauserpwdencrypted : boolean, optional
        Specifies that tdatauserpwd is encrypted

    Returns
    -------
    :class:`TeradataSubscriber`

    '''
    connector_key = dict(cls='tdata', type='subscribe')

    property_defs = dict(
        tdatatdpid=prop('tdatatdpid', dtype='string', required=True),
        desttablename=prop('desttablename', dtype='string', required=True),
        tdatausername=prop('tdatausername', dtype='string', required=True),
        tdatauserpwd=prop('tdatauserpwd', dtype='string', required=True),
        tdatamaxsessions=prop('tdatamaxsessions', dtype='int', required=True),
        tdataminsessions=prop('tdataminsessions', dtype='int', required=True),
        tdatadriver=prop('tdatadriver', dtype='string', required=True),
        tdatainsertonly=prop('tdatainsertonly', dtype='boolean', required=True),
        snapshot=prop('snapshot', dtype='boolean', required=True, default=False),
        rmretdel=prop('rmretdel', dtype='boolean'),
        tdatabatchperiod=prop('tdatabatchperiod', dtype='int'),
        stage1tablename=prop('stage1tablename', dtype='string'),
        stage2tablename=prop('stage2tablename', dtype='string'),
        connectstring=prop('connectstring', dtype='string'),
        tdatatracelevel=prop('tdatatracelevel', dtype='int'),
        configfilesection=prop('configfilesection', dtype='string'),
        tdatauserpwdencrypted=prop('tdatauserpwdencrypted', dtype='boolean')
    )

    def __init__(self, tdatatdpid=None, desttablename=None, tdatausername=None,
                 tdatauserpwd=None, tdataminsessions=None, tdatamaxsessions=None,
                 tdatadriver=None, tdatainsertonly=None,
                 name=None, is_active=None, snapshot=None,
                 rmretdel=None, tdatabatchperiod=None, stage1tablename=None,
                 stage2tablename=None, connectstring=None, tdatatracelevel=None,
                 configfilesection=None, tdatauserpwdencrypted=None):
        params = dict(**locals())
        params.pop('is_active')
        params.pop('self')
        name = params.pop('name')
        Connector.__init__(self, 'tdata', name=name, type='subscribe',
                           is_active=is_active, properties=params)

    @classmethod
    def from_parameters(cls, conncls, type=None, name=None, is_active=None,
                        properties=None):
        req, properties = map_properties(cls, properties,
                                         required=['tdatatdpid',
                                                   'desttablename',
                                                   'tdatausername',
                                                   'tdatauserpwd',
                                                   'tdataminsessions',
                                                   'tdatamaxsessions',
                                                   'tdatadriver',
                                                   'tdatainsertonly'],
                                         delete='type')
        return cls(req[0], req[1], req[2], req[3], req[4], req[5], req[6],
                   req[7], name=name, is_active=is_active, **properties)


class TeradataListenerSubscriber(Connector):
    '''
    Subscribe to Teradata Listener events

    Parameters
    ----------
    ingestUrl : string
        Specifies the URL for the Listener Ingest REST API (version 1)
    SSLCert : strirng
        Specifies the path to a file that contains SSL certificates securely
        connect to the Listener Ingest service. Listener uses TLS 1.2.
    sourceKey : string
        Specifies the Listener source secret key that identifies the
        Listener source feed to which Event Stream Processing sends data.
    ingestBlocksize : int, optional
        Specifies the maximum number of data rows to send in one Listener
        Ingest message. Matching the connector block size to the Source
        window block size is recommended. The default block size is 256.
    contentType : string, optional
        Specifies the format of the data sent from Event Stream Processing
        to Listener, either JSON or plaintext (comma-delimited).
        The default is JSON.
    ingestDelim : string, optional
        Specifies the character that delimits data rows in a multi-row
        message from Event Stream Processing to the Listener Ingest
        REST API. The delimiter must not be a JSON punctuation character.
        The default is a tilde (~).
    snapshot : boolean, optional
         Specifies whether to send snapshot data.

    Returns
    -------
    :class:`TeradataListenerSubscriber`

    '''
    connector_key = dict(cls='tdlistener', type='subscribe')

    property_defs = dict(
        ingestUrl=prop('ingestUrl', dtype='string', required=True),
        SSLCert=prop('SSLCert', dtype='string', required=True),
        sourceKey=prop('sourceKey', dtype='string', required=True),
        snapshot=prop('snapshot', dtype='boolean', required=True, default=False),
        ingestBlocksize=prop('ingestBlocksize', dtype='int'),
        contentType=prop('contentType', dtype='string'),
        ingestDelim=prop('ingestDelim', dtype='string')
    )

    def __init__(self, ingestUrl, SSLCert, sourceKey, name=None,
                 is_active=None, snapshot=None, ingestBlocksize=None,
                 contentType=None, ingestDelim=None):
        params = dict(**locals())
        params.pop('is_active')
        params.pop('self')
        name = params.pop('name')
        Connector.__init__(self, 'tdlistener', name=name, type='subscribe',
                           is_active=is_active, properties=params)

    @classmethod
    def from_parameters(cls, conncls, type=None, name=None, is_active=None,
                        properties=None):
        req, properties = map_properties(cls, properties,
                                         required=['ingestUrl',
                                                   'SSLCert',
                                                   'sourceKey'],
                                         delete='type')
        return cls(req[0], req[1], req[2], name=name, is_active=is_active, **properties)
