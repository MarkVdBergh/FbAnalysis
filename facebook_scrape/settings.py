# mongodb settings
testing = True
TEST_DATABASE = 'test'
PRODUCTION_DATABASE = 'production'
if not testing:
    print '*' * 100
    print ' ' * 20 + 'WARNING !!!!'
    print ' ' * 20 + 'WORKING ON PRODUCTION DATABASE'
    print '*' * 100
    DB = _production_database
else:
    DB = TEST_DATABASE

MONGO_HOST = 'localhost'
MONGO_PORT = 27017

# facebook graph_api settings
FB_APP_ID = '765646503583435'
FB_APP_SECRET = 'e4fbccb989f8f898f6c5336d7ea46d47'

# Localization settings
LOCAL_TIMEZONE = 'Europe/Brussels'
LOCAL_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
