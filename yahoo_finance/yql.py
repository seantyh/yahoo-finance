"""
Yahoo! Python SDK

 * Yahoo! Query Language
 * Yahoo! Social API

Find documentation and support on Yahoo! Developer Network: http://developer.yahoo.com

Hosted on GitHub: http://github.com/yahoo/yos-social-python/tree/master

@copyright: Copyrights for code authored by Yahoo! Inc. is licensed under the following terms:
@license:   BSD Open Source License

  Permission is hereby granted, free of charge, to any person obtaining a copy
  of this software and associated documentation files (the "Software"), to deal
  in the Software without restriction, including without limitation the rights
  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
  copies of the Software, and to permit persons to whom the Software is
  furnished to do so, subject to the following conditions:

  The above copyright notice and this permission notice shall be included in
  all copies or substantial portions of the Software.

  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
  THE SOFTWARE.
"""

__author__   = 'Dustin Whittle <dustin@yahoo-inc.com>'
__version__  = '0.1'

try:
    from http.client import HTTPConnection
except ImportError:
    from httplib import HTTPConnection
try:
    from urllib.parse import urlencode
except ImportError:
    from urllib import urlencode

import simplejson
import time
from datetime import datetime, timedelta
import logging

# Yahoo! YQL API
PUBLIC_API_URL  = 'http://query.yahooapis.com/v1/public/yql'
OAUTH_API_URL   = 'http://query.yahooapis.com/v1/yql'
DATATABLES_URL  = 'store://datatables.org/alltableswithkeys'

class YQLQuery(object):
  RETRY_MAX = 3
  DELAY_TIME = timedelta(seconds = 3)

  def __init__(self):
    self.connection = HTTPConnection('query.yahooapis.com')
    self.retry_count = 0
    self.cool_down = None

  def execute(self, yql, token = None):
    if self.cool_down is not None:
        delta = datetime.now() - self.cool_down
        if delta < YQLQuery.DELAY_TIME:
            logging.debug("waiting for next request")
            time.sleep(YQLQuery.DELAY_TIME - delta)
    self.cool_down = datetime.now()
    logging.debug("sending request, %s" % yql)
    self.connection.request('GET', PUBLIC_API_URL + '?' + urlencode({ 'q': yql, 'format': 'json', 'env': DATATABLES_URL }))
    resp_content = self.connection.getresponse().read()
    try:
        return simplejson.loads(resp_content)
    except simplejson.JSONDecodeError as ex:
        logging.debug(ex)
        if self.retry_count < YQLQuery.RETRY_MAX:
            logging.debug("Retrying...")
            time.sleep(1)        
            self.retry_count += 1
            return self.execute(yql, token)
        else:
            logging.debug("Max retry_count is reached")
            return []

  def __del__(self):
    self.connection.close()
