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

# NOTE: This test requires a running ESP server.  You must use an ~/.authinfo
#       file to specify your username and password.  The ESP host and port must
#       be specified using the ESPHOST and ESPPORT environment variables.
#       A specific protocol ('http' or 'https') can be set using
#       the ESPPROTOCOL environment variable.

import os
import unittest
from . import utils as tm


SMTP_SERVER = '@ESP_SMTP_SERVER@'
ESP_EMAIL = '@ESP_EMAIL@'


class TestNotificationWindow(tm.WindowTestCase):

    def test_xml(self):
        self._test_model_file('notification_window.xml')

    def test_api(self):
        esp = self.s

        proj = esp.create_project('project_01_UnitTest',pubsub='auto',n_threads='1')

        cq = esp.ContinuousQuery(name='cq_01')

        src = esp.SourceWindow(name='src_win',
                               schema=('id*:int32','symbol:string','price:double',
                                       'quant:int64','broker:string'),
                               output_insert_only=True, collapse_updates=True)
        src.add_connector('fs', conn_name='pub', conn_type='publish',
                                type='pub',
                                fstype='csv',
                                fsname=os.path.join(tm.DATA_DIR, 'notification_window.csv'))

        notif_win = esp.NotificationWindow(name='notif_win',
                                           schema=('symbol:string','quant:double',
                                                   'price:double','email:string','broker:string'))
        notif_win.set_smtp_settings(SMTP_SERVER)
        notif_win.set_function_context_functions(broker='string($broker)',
            email='rgxReplace($email,\'esptest@mycompany.com\',\'%s\')' % ESP_EMAIL)

        notif_win.add_email('$email', '$email', 'notificationEmailSchema',
                            deliver=True, from_='esp', to='trader',
                            text='broker=$broker symbol=$symbol at $price.',
                            html='<html><head><title>Page Title</title></head>'
                                 '<body><h1>This is a Heading</h1>'
                                 '<p>This is a paragraph.</p></body></html>',
                            image='file:///tmp/Desert.jpeg', test=True)
        notif_win.add_email('$email', '$email', 'notificationEmailSchema',
                            deliver=True, from_='esp', to='trader',
                            text='broker=$broker symbol=$symbol at $price.',
                            html='<html><head><title>Page Title</title></head>'
                                 '<body><h1>This is a Heading</h1>'
                                 '<p>This is a paragraph.</p></body></html>', test=True)

        proj.add_query(cq)
        cq.add_window(src)
        cq.add_window(notif_win)

        src.add_target(notif_win)

        self._test_model_file('notification_window.xml', proj)

    def test_smtp_xml(self):
        proj = self._load_project('notification_window.xml')
        xml = proj['cq_01']['notif_win'].smtp.to_xml(pretty=True)
        self.assertEqual(xml, '<smtp host="%s" />' % SMTP_SERVER)


class TestEmailNotification(tm.WindowTestCase):

    # source : notification / notificationEmailSchema

    def test_xml(self):
        self._test_model_file('email_notification.xml')

    def test_api(self):
        esp = self.s

        proj = esp.create_project('project_01_UnitTest', pubsub='auto', n_threads='1')

        cq = esp.ContinuousQuery(name='contquery')

        notif_win = esp.NotificationWindow(name='notify',
                                           schema=('symbol:string','quant:double',
                                                   'price:double','email:string'))
        notif_win.set_smtp_settings(SMTP_SERVER)
        notif_win.add_email('$email', '$email', 'notificationEmailSchema',
                            deliver=True, from_='esp', to='trader',
                            text='I see you traded $quant shares of $symbol at $price. You must have a lot of money. Can I interest you in a timeshare?',
                            html=r'''<!DOCTYPE html>
                 <html>
                 <head>
                   <title>Page Title</title>
                   </head>

<body>
  <h1>This is a Heading</h1>
    <p>This is a paragraph.</p>
    </body>

</html>''',
                            image='file:///tmp/Desert.jpeg', test=True)

        proj.add_query(cq)
        cq.add_window(notif_win)

        self._test_model_file('email_notification.xml', proj)

    def test_copy(self):
        self._test_copy('email_notification.xml', 'contquery', 'notify')

    def test_xml(self):
        proj = self._load_project('email_notification.xml')
        xml = proj['contquery']['notify'].email[0].to_xml(pretty=True)
        self.assertEqual(xml.strip(), r'''<email test="true">
  <deliver>1</deliver>
  <email-info>
    <sender>$email</sender>
    <recipients>$email</recipients>
    <subject>notificationEmailSchema</subject>
    <from>esp</from>
    <to>trader</to>
  </email-info>
  <email-contents>
    <text-content name="text_content_0">I see you traded $quant shares of $symbol at $price. You must have a lot of money. Can I interest you in a timeshare?</text-content>
    <html-content name="html_content_0">&lt;!DOCTYPE html&gt;
                 &lt;html&gt;
                 &lt;head&gt;
                   &lt;title&gt;Page Title&lt;/title&gt;
                   &lt;/head&gt;

&lt;body&gt;
  &lt;h1&gt;This is a Heading&lt;/h1&gt;
    &lt;p&gt;This is a paragraph.&lt;/p&gt;
    &lt;/body&gt;

&lt;/html&gt; </html-content>
    <image-content name="image_content_0">file:///tmp/Desert.jpeg</image-content>
  </email-contents>
</email>''')


class TestSMSNotification(tm.WindowTestCase):

    # source : notification / notificationSMS

    def test_xml(self):
        self._test_model_file('sms_notification.xml')

    def test_api(self):
        esp = self.s

        proj = esp.create_project('project_01_UnitTest', pubsub='auto', n_threads='1')

        cq = esp.ContinuousQuery(name='contquery')

        notif_win = esp.NotificationWindow(name='notify')
        notif_win.set_smtp_settings(SMTP_SERVER)
        notif_win.add_sms('$email', 'notificationSMS', 'esp', 'txt.att.net', '1234567890',
                          deliver=True,
                          text='I see you traded $quant shares of $symbol at $price. You must have a lot of money. Can I interest you in a timeshare?',
                          test=True)

        proj.add_query(cq)
        cq.add_window(notif_win)

        self._test_model_file('sms_notification.xml', proj)

    def test_copy(self):
        self._test_copy('sms_notification.xml', 'contquery', 'notify')

    def test_xml(self):
        proj = self._load_project('sms_notification.xml')
        xml = proj['contquery']['notify'].sms[0].to_xml(pretty=True)
        self.assertEqual(xml.strip(), r'''<sms test="true">
  <deliver>1</deliver>
  <sms-info>
    <sender>$email</sender>
    <subject>notificationSMS</subject>
    <from>esp</from>
    <gateway>txt.att.net</gateway>
    <phone>1234567890</phone>
  </sms-info>
  <sms-contents>
    <text-content name="text_content_0">I see you traded $quant shares of $symbol at $price. You must have a lot of money. Can I interest you in a timeshare?</text-content>
  </sms-contents>
</sms>''')


class TestMMSNotification(tm.WindowTestCase):

    # source : notification / notificationMMS

    def test_xml(self):
        self._test_model_file('mms_notification.xml')

    def test_api(self):
        esp = self.s

        proj = esp.create_project('project_01_UnitTest', pubsub='auto', n_threads='1')

        cq = esp.ContinuousQuery(name='contquery')

        notif_win = esp.NotificationWindow(name='notify')
        notif_win.set_smtp_settings(SMTP_SERVER)
        notif_win.add_mms('$email', 'notificationMMS', 'esp', 'mms.att.net', '1234567890',
                          deliver=True,
                          text='I see you traded $quant shares of $symbol at $price. You must have a lot of money. Can I interest you in a timeshare?',
                          image='file:///tmp/d.jpeg',
                          test=True)

        proj.add_query(cq)
        cq.add_window(notif_win)

        self._test_model_file('mms_notification.xml', proj)

    def test_copy(self):
        self._test_copy('mms_notification.xml', 'contquery', 'notify')

    def test_xml(self):
        proj = self._load_project('mms_notification.xml')
        xml = proj['contquery']['notify'].mms[0].to_xml(pretty=True)
        self.assertEqual(xml.strip(), r'''<mms test="true">
  <deliver>1</deliver>
  <mms-info>
    <sender>$email</sender>
    <subject>notificationMMS</subject>
    <from>esp</from>
    <gateway>mms.att.net</gateway>
    <phone>1234567890</phone>
  </mms-info>
  <mms-contents>
    <text-content name="text_content_0">I see you traded $quant shares of $symbol at $price. You must have a lot of money. Can I interest you in a timeshare?</text-content>
    <image-content name="image_content_0">file:///tmp/d.jpeg</image-content>
  </mms-contents>
</mms>''')
