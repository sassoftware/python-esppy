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

from __future__ import print_function, division, absolute_import, unicode_literals

import re
from .base import BaseWindow, attribute
from .features import (FinalizedCallbackFeature, SchemaFeature,
                       FunctionContextFeature)
from .utils import get_args, ensure_element, listify, connectors_to_end
from ..utils import xml


class SMTPSettings(object):
    '''
    SMTP server settings

    Parameters
    ----------
    host : string
        SMTP host name
    port : int, optional
        SMTP port number
    user : string, optional
        User name
    password : string, optional
        Password

    Returns
    -------
    :class:`SMTPSettings`

    '''

    def __init__(self, host, port=None, user=None, password=None):
        self.host = host
        self.port = port
        self.user = user
        self.password = password

    def copy(self, deep=False):
        return type(self)(self.host, port=self.port, user=self.user,
                          password=self.password)

    @classmethod
    def from_element(cls, data, session=None):
        '''
        Convert XML / Element to object

        Parameters
        ----------
        data : xml-string or Element
            The element to convert
        session : Session, optional
            The requests.Session object

        Returns
        -------
        :class:`SMTPSettings`

        '''
        data = ensure_element(data)
        out = cls(data.attrib['host'])
        out.user = data.attrib.get('user')
        out.password = data.attrib.get('password')
        out.port = data.attrib.get('port')
        if out.port is not None:
            out.port = int(out.port)
        return out

    from_xml = from_element

    def to_element(self):
        '''
        Convert object to Element

        Returns
        -------
        :class:`ElementTree.Element`

        '''
        return xml.new_elem('smtp',
                            attrib=dict(host=self.host, port=self.port,
                                        user=self.user, password=self.password))

    def to_xml(self, pretty=False):
        '''
        Convert object to XML

        Parameters
        ----------
        pretty : bool, optional
            Should whitespace be added for readability?

        Returns
        -------
        string

        '''
        return xml.to_xml(self.to_element(), pretty=pretty)


class EMailMessage(object):
    '''
    EMail notifier

    Parameters
    ----------
    sender : string
        The address of the message sender
    recipients : list-of-strings
        The addresses of the message recipients
    subject : string
        The subject of the message
    from_ : string, optional
        The string to display in the From field of the message
    to : string, optional
        The string to display in the To field of the message
    text : string, optional
        The text of the message
    html : string, optional
        The HTML content of the message
    image : string, optional
        The image content of the message
    deliver : string, optional
        Enclosed text specifies a function to run in order
        to determine whether a notification should be sent.
    name : string, optional
        The name of the notification window
    unresolved_text : string, optional
        Specifies the text to use when the token cannot be resolved.
    throttle_interval : string, optional
        Specifies a time period in which at most one notification
        is sent to a recipient.
    test : boolean, optional
        Specifies whether to run in test mode.

    Returns
    -------
    :class:`EMailMessage`

    '''

    def __init__(self, sender, recipients, subject, from_=None, to=None,
                 text=None, html=None, image=None, deliver=True, name=None,
                 unresolved_text=None, throttle_interval=None, test=False):
        self.sender = sender
        self.recipients = listify(recipients) or []
        self.subject = subject
        self.from_ = from_
        self.to = listify(to) or []
        self.text = listify(text) or []
        self.html = listify(html) or []
        self.image = listify(image) or []
        self.deliver = deliver
        self.name = name
        self.unresolved_text = unresolved_text
        self.throttle_interval = throttle_interval
        self.test = test
        if self.from_ is None:
            self.from_ = sender
        if not self.to:
            self.to = self.recipients

    def copy(self, deep=False):
        return type(self)(self.sender, self.recipients, self.subject, from_=self.from_,
                          to=self.to, text=self.text, html=self.html, image=self.image,
                          deliver=self.deliver, name=self.name,
                          unresolved_text=self.unresolved_text,
                          throttle_interval=self.throttle_interval, test=self.test)

    @classmethod
    def from_element(cls, data, session=None):
        '''
        Convert XML / Element to object

        Parameters
        ----------
        data : xml-string or Element
            The element to convert
        session : Session, optional
            The requests.Session object

        Returns
        -------
        :class:`EMailMessage`

        '''
        data = ensure_element(data)
        out = cls('', '', '')

        out.name = data.attrib.get('name')
        out.unresolved_text = data.attrib.get('unresolved-text')
        out.throttle_interval = data.attrib.get('throttle-interval')
        out.test = data.attrib.get('test', False)

        for item in data.findall('./deliver'):
            out.deliver = item.text

        for item in data.findall('./email-info/sender'):
            out.sender = item.text
        for item in data.findall('./email-info/recipients'):
            out.recipients = re.split(r'\s*,\s*', item.text.strip())
        for item in data.findall('./email-info/subject'):
            out.subject = item.text
        for item in data.findall('./email-info/from'):
            out.from_ = item.text
        for item in data.findall('./email-info/to'):
            out.to = re.split(r'\s*,\s*', item.text.strip())

        for item in data.findall('./email-contents/text-content'):
            out.text.append(item.text)
        for item in data.findall('./email-contents/html-content'):
            out.html.append(item.text)
        for item in data.findall('./email-contents/image-content'):
            out.image.append(item.text)

        return out

    from_xml = from_element

    def to_element(self):
        '''
        Convert object to Element

        Returns
        -------
        :class:`ElementTree.Element`

        '''
        out = xml.new_elem('email', attrib=dict(name=self.name,
                                                unresolved_text=self.unresolved_text,
                                                throttle_interval=self.throttle_interval,
                                                test=self.test))

        xml.add_elem(out, 'deliver', text_content=int(self.deliver))

        info = xml.add_elem(out, 'email-info')
        xml.add_elem(info, 'sender', text_content=self.sender)
        xml.add_elem(info, 'recipients', text_content=','.join(self.recipients))
        xml.add_elem(info, 'subject', text_content=self.subject)
        xml.add_elem(info, 'from', text_content=self.from_)
        xml.add_elem(info, 'to', text_content=', '.join(self.to))

        contents = xml.add_elem(out, 'email-contents')
        for i, item in enumerate(self.text):
            xml.add_elem(contents, 'text-content',
                         attrib=dict(name='text_content_%s' % i),
                         text_content=item)
        for i, item in enumerate(self.html):
            xml.add_elem(contents, 'html-content',
                         attrib=dict(name='html_content_%s' % i),
                         text_content=item)
        for i, item in enumerate(self.image):
            xml.add_elem(contents, 'image-content',
                         attrib=dict(name='image_content_%s' % i),
                         text_content=item)

        return out

    def to_xml(self, pretty=False):
        '''
        Convert object to XML

        Parameters
        ----------
        pretty : bool, optional
            Should whitespace be added for readability?

        Returns
        -------
        string

        '''
        return xml.to_xml(self.to_element(), pretty=pretty)


class SMSMessage(object):
    '''
    SMS notifier

    Parameters
    ----------
    sender : string
        The address of the message sender
    subject : string
        The subject of the message
    from_ : string, optional
        The string to display in the From field of the message
    gateway : string, optional
        Specifies the recipient's provider's SMS gateway
    phone : string, optional
        The phone number to send message to
    text : string, optional
        The text of the message
    deliver : string, optional
        Enclosed text specifies a function to run in order
        to determine whether a notification should be sent.
    name : string, optional
        The name of the notification window
    unresolved_text : string, optional
        Specifies the text to use when the token cannot be resolved.
    throttle_interval : string, optional
        Specifies a time period in which at most one notification
        is sent to a recipient.
    test : boolean, optional
        Specifies whether to run in test mode.

    Returns
    -------
    :class:`SMSMessage`

    '''

    def __init__(self, sender, subject, from_=None, gateway=None, phone=None,
                 text=None, deliver=None, name=None, unresolved_text=None,
                 throttle_interval=None, test=None):
        self.sender = sender
        self.subject = subject
        self.from_ = from_
        self.gateway = gateway
        self.phone = phone
        self.text = listify(text) or []
        self.deliver = deliver
        self.name = name
        self.unresolved_text = unresolved_text
        self.throttle_interval = throttle_interval
        self.test = test

    def copy(self, deep=False):
        return type(self)(self.sender, self.subject, from_=self.from_,
                          gateway=self.gateway, phone=self.phone, text=self.text,
                          deliver=self.deliver, name=self.name,
                          unresolved_text=self.unresolved_text,
                          throttle_interval=self.throttle_interval, test=self.test)

    @classmethod
    def from_element(cls, data, session=None):
        '''
        Convert XML / Element to object

        Parameters
        ----------
        data : xml-string or Element
            The element to convert
        session : Session, optional
            The requests.Session object

        Returns
        -------
        :class:`SMSMessage`

        '''
        data = ensure_element(data)
        out = cls('', '')

        out.name = data.attrib.get('name')
        out.unresolved_text = data.attrib.get('unresolved-text')
        out.throttle_interval = data.attrib.get('throttle-interval')
        out.test = data.attrib.get('test', False)

        for item in data.findall('./deliver'):
            out.deliver = item.text

        for item in data.findall('./sms-info/sender'):
            out.sender = item.text
        for item in data.findall('./sms-info/subject'):
            out.subject = item.text
        for item in data.findall('./sms-info/from'):
            out.from_ = item.text
        for item in data.findall('./sms-info/gateway'):
            out.gateway = item.text
        for item in data.findall('./sms-info/phone'):
            out.phone = item.text

        for item in data.findall('./sms-contents/text-content'):
            out.text.append(item.text)

        return out

    from_xml = from_element

    def to_element(self):
        '''
        Convert object to Element

        Returns
        -------
        :class:`ElementTree.Element`

        '''
        out = xml.new_elem('sms', attrib=dict(name=self.name,
                                              unresolved_text=self.unresolved_text,
                                              throttle_interval=self.throttle_interval,
                                              test=self.test))

        xml.add_elem(out, 'deliver', text_content=int(self.deliver))

        info = xml.add_elem(out, 'sms-info')
        xml.add_elem(info, 'sender', text_content=self.sender)
        xml.add_elem(info, 'subject', text_content=self.subject)
        xml.add_elem(info, 'from', text_content=self.from_)
        xml.add_elem(info, 'gateway', text_content=self.gateway)
        xml.add_elem(info, 'phone', text_content=self.phone)

        contents = xml.add_elem(out, 'sms-contents')
        for i, item in enumerate(self.text):
            xml.add_elem(contents, 'text-content',
                         attrib=dict(name='text_content_%s' % i),
                         text_content=item)

        return out

    def to_xml(self, pretty=False):
        '''
        Convert object to XML

        Parameters
        ----------
        pretty : bool, optional
            Should whitespace be added for readability?

        Returns
        -------
        string

        '''
        return xml.to_xml(self.to_element(), pretty=pretty)


class MMSMessage(object):
    '''
    Add an SMS notifier

    Parameters
    ----------
    sender : string
        The address of the message sender
    subject : string
        The subject of the message
    from_ : string, optional
        The string to display in the From field of the message
    gateway : string, optional
        Specifies the recipient's provider's SMS gateway
    phone : string, optional
        The phone number to send message to
    text : string, optional
        The text of the message
    image : string, optional
        The image content of the message
    deliver : string, optional
        Enclosed text specifies a function to run in order
        to determine whether a notification should be sent.
    name : string, optional
        The name of the notification window
    unresolved_text : string, optional
        Specifies the text to use when the token cannot be resolved.
    throttle_interval : string, optional
        Specifies a time period in which at most one notification
        is sent to a recipient.
    test : boolean, optional
        Specifies whether to run in test mode.

    '''

    def __init__(self, sender, subject, from_=None, gateway=None, phone=None,
                 text=None, image=None, deliver=None, name=None,
                 unresolved_text=None, throttle_interval=None, test=None):
        self.sender = sender
        self.subject = subject
        self.from_ = from_
        self.gateway = gateway
        self.phone = phone
        self.text = listify(text) or []
        self.image = listify(image) or []
        self.deliver = deliver
        self.name = name
        self.unresolved_text = unresolved_text
        self.throttle_interval = throttle_interval
        self.test = test

    def copy(self, deep=False):
        return type(self)(self.sender, self.subject, from_=self.from_,
                          gateway=self.gateway, phone=self.phone, text=self.text,
                          image=self.image, deliver=self.deliver, name=self.name,
                          unresolved_text=self.unresolved_text,
                          throttle_interval=self.throttle_interval, test=self.test)

    @classmethod
    def from_element(cls, data, session=None):
        '''
        Convert XML / Element to object

        Parameters
        ----------
        data : xml-string or Element
            The element to convert
        session : Session, optional
            The requests.Session object

        Returns
        -------
        :class:`MMSMessage`

        '''
        data = ensure_element(data)
        out = cls('', '')

        out.name = data.attrib.get('name')
        out.unresolved_text = data.attrib.get('unresolved-text')
        out.throttle_interval = data.attrib.get('throttle-interval')
        out.test = data.attrib.get('test', False)

        for item in data.findall('./deliver'):
            out.deliver = item.text

        for item in data.findall('./mms-info/sender'):
            out.sender = item.text
        for item in data.findall('./mms-info/subject'):
            out.subject = item.text
        for item in data.findall('./mms-info/from'):
            out.from_ = item.text
        for item in data.findall('./mms-info/gateway'):
            out.gateway = item.text
        for item in data.findall('./mms-info/phone'):
            out.phone = item.text

        for item in data.findall('./mms-contents/text-content'):
            out.text.append(item.text)
        for item in data.findall('./mms-contents/image-content'):
            out.image.append(item.text)

        return out

    from_xml = from_element

    def to_element(self):
        '''
        Convert object to Element

        Returns
        -------
        :class:`ElementTree.Element`

        '''
        out = xml.new_elem('mms', attrib=dict(name=self.name,
                                              unresolved_text=self.unresolved_text,
                                              throttle_interval=self.throttle_interval,
                                              test=self.test))

        xml.add_elem(out, 'deliver', text_content=int(self.deliver))

        info = xml.add_elem(out, 'mms-info')
        xml.add_elem(info, 'sender', text_content=self.sender)
        xml.add_elem(info, 'subject', text_content=self.subject)
        xml.add_elem(info, 'from', text_content=self.from_)
        xml.add_elem(info, 'gateway', text_content=self.gateway)
        xml.add_elem(info, 'phone', text_content=self.phone)

        contents = xml.add_elem(out, 'mms-contents')
        for i, item in enumerate(self.text):
            xml.add_elem(contents, 'text-content',
                         attrib=dict(name='text_content_%s' % i),
                         text_content=item)
        for i, item in enumerate(self.image):
            xml.add_elem(contents, 'image-content',
                         attrib=dict(name='image_content_%s' % i),
                         text_content=item)

        return out

    def to_xml(self, pretty=False):
        '''
        Convert object to XML

        Parameters
        ----------
        pretty : bool, optional
            Should whitespace be added for readability?

        Returns
        -------
        string

        '''
        return xml.to_xml(self.to_element(), pretty=pretty)


class NotificationWindow(BaseWindow, FinalizedCallbackFeature,
                         SchemaFeature, FunctionContextFeature):
    '''
    Notification window

    Parameters
    ----------
    name : string, optional
        The name of the window
    schema : Schema, optional
        The schema of the window

    Attributes
    ----------
    smtp : SMTPSettings
        The SMTP server settings
    email : list-of-EMailMessages
        The email messages to send
    sms : list-of-SMSMessages
        The SMS messages to send
    mms : list-of-MMSMessages
        The MMS messages to send

    Returns
    -------
    :class:`NotificationWindow`

    '''

    window_type = 'notification'

    def __init__(self, name=None, schema=None, pubsub=None, description=None):
        BaseWindow.__init__(self, **get_args(locals()))
        self.smtp = SMTPSettings('localhost')
        self.email = []
        self.sms = []
        self.mms = []

    def copy(self, deep=False):
        out = BaseWindow.copy(self, deep=deep)
        out.smtp = self.smtp.copy(deep=deep)
        if deep:
            out.email = [x.copy(deep=deep) for x in self.email]
            out.sms = [x.copy(deep=deep) for x in self.sms]
            out.mms = [x.copy(deep=deep) for x in self.mms]
        else:
            out.email = list(self.email)
            out.sms = list(self.sms)
            out.mms = list(self.mms)
        return out

    def set_smtp_settings(self, host, port=None, user=None, password=None):
        '''
        Set the SMTP server settings

        Parameters
        ----------
        host : string
            The hostname of the SMTP server
        port : int, optional
            The SMTP server port
        user : string, optional
            The user name on the SMTP server
        password : string, optional
            The password on the SMTP server

        '''
        self.smtp = SMTPSettings(host, port=port, user=user, password=password)

    def add_email(self, sender, recipients, subject, from_=None, to=None,
                  text=None, html=None, image=None, deliver=True, name=None,
                  unresolved_text=None, throttle_interval=None, test=False):
        '''
        Add an email notifier

        Parameters
        ----------
        sender : string
            The address of the message sender
        recipients : list-of-strings
            The addresses of the message recipients
        subject : string
            The subject of the message
        from_ : string, optional
            The string to display in the From field of the message
        to : string, optional
            The string to display in the To field of the message
        text : string, optional
            The text of the message
        html : string, optional
            The HTML content of the message
        image : string, optional
            The image content of the message
        deliver : string, optional
            Enclosed text specifies a function to run in order
            to determine whether a notification should be sent.
        name : string, optional
            The name of the notification window
        unresolved_text : string, optional
            Specifies the text to use when the token cannot be resolved.
        throttle_interval : string, optional
            Specifies a time period in which at most one notification
            is sent to a recipient.
        test : boolean, optional
            Specifies whether to run in test mode.

        '''
        self.email.append(
            EMailMessage(sender, recipients, subject, from_=from_, to=to,
                         text=text, html=html, image=image, deliver=deliver,
                         name=name, unresolved_text=unresolved_text,
                         throttle_interval=throttle_interval, test=test))

    def add_sms(self, sender, subject, from_, gateway, phone, text=None, deliver=True,
                name=None, unresolved_text=None, throttle_interval=None, test=False):
        '''
        Add an SMS notifier

        Parameters
        ----------
        sender : string
            The address of the message sender
        subject : string
            The subject of the message
        from_ : string, optional
            The string to display in the From field of the message
        gateway : string, optional
            Specifies the recipient's provider's SMS gateway
        phone : string, optional
            The phone number to send message to
        text : string, optional
            The text of the message
        deliver : string, optional
            Enclosed text specifies a function to run in order
            to determine whether a notification should be sent.
        name : string, optional
            The name of the notification window
        unresolved_text : string, optional
            Specifies the text to use when the token cannot be resolved.
        throttle_interval : string, optional
            Specifies a time period in which at most one notification
            is sent to a recipient.
        test : boolean, optional
            Specifies whether to run in test mode.

        '''
        self.sms.append(
            SMSMessage(sender, subject, from_=from_, gateway=gateway, phone=phone,
                       text=text, deliver=deliver, name=name,
                       unresolved_text=unresolved_text,
                       throttle_interval=throttle_interval, test=test))

    def add_mms(self, sender, subject, from_, gateway, phone, text=None, image=None,
                deliver=True, name=None, unresolved_text=None, throttle_interval=None,
                test=False):
        '''
        Add an SMS notifier

        Parameters
        ----------
        sender : string
            The address of the message sender
        subject : string
            The subject of the message
        from_ : string, optional
            The string to display in the From field of the message
        gateway : string, optional
            Specifies the recipient's provider's SMS gateway
        phone : string, optional
            The phone number to send message to
        text : string, optional
            The text of the message
        image : string, optional
            The image content of the message
        deliver : string, optional
            Enclosed text specifies a function to run in order
            to determine whether a notification should be sent.
        name : string, optional
            The name of the notification window
        unresolved_text : string, optional
            Specifies the text to use when the token cannot be resolved.
        throttle_interval : string, optional
            Specifies a time period in which at most one notification
            is sent to a recipient.
        test : boolean, optional
            Specifies whether to run in test mode.

        '''
        self.mms.append(
            MMSMessage(sender, subject, from_=from_, gateway=gateway, phone=phone,
                       text=text, image=image, deliver=deliver, name=name,
                       unresolved_text=unresolved_text,
                       throttle_interval=throttle_interval, test=test))

    @classmethod
    def from_element(cls, data, session=None):
        '''
        Convert XML / Element to object

        Parameters
        ----------
        data : xml-string or Element
            The element to convert
        session : Session, optional
            The requests.Session object

        Returns
        -------
        :class:`NotificationWindow`

        '''
        data = ensure_element(data)
        out = super(NotificationWindow, cls).from_element(data, session=session)

        for item in data.findall('./smtp'):
            out.smtp = SMTPSettings.from_element(item, session=session)

        for item in data.findall('./delivery-channels/email'):
            out.email.append(EMailMessage.from_element(item, session=session))

        for item in data.findall('./delivery-channels/sms'):
            out.sms.append(SMSMessage.from_element(item, session=session))

        for item in data.findall('./delivery-channels/mms'):
            out.mms.append(MMSMessage.from_element(item, session=session))

        return out

    from_xml = from_element

    def to_element(self, query=None):
        '''
        Convert object to Element

        Parameters
        ----------
        query : string, optional
            The name of the continuous query

        Returns
        -------
        :class:`ElementTree.Element`

        '''
        out = BaseWindow.to_element(self, query=query)

        xml.add_elem(out, self.smtp.to_element())
        channels = xml.add_elem(out, 'delivery-channels')

        for email in self.email:
            xml.add_elem(channels, email.to_element())

        for sms in self.sms:
            xml.add_elem(channels, sms.to_element())

        for mms in self.mms:
            xml.add_elem(channels, mms.to_element())

        connectors_to_end(out)

        return out
